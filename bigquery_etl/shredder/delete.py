"""Delete user data from long term storage."""

from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta
import asyncio
import logging
import warnings

from google.cloud import bigquery

from ..util.client_queue import ClientQueue
from ..util.temp_table import get_temporary_table
from ..util.table_filter import add_table_filter_arguments, get_table_filter
from .config import DELETE_TARGETS


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "-n",
    "--dry_run",
    "--dry-run",
    action="store_true",
    help="Do not make changes, only log actions that would be taken; "
    "use with --log-level=DEBUG to log query contents",
)
parser.add_argument(
    "-l",
    "--log-level",
    "--log_level",
    default=logging.getLevelName(logging.INFO),
    type=str.upper,
)
parser.add_argument(
    "-P",
    "--parallelism",
    default=4,
    type=int,
    help="Maximum number of queries to execute concurrently",
)
parser.add_argument(
    "-e",
    "--end-date",
    "--end_date",
    default=datetime.utcnow().date(),
    type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
    help="last date of last date of pings to delete; One day after last date of "
    "deletion requests to process; defaults to today in UTC",
)
parser.add_argument(
    "-s",
    "--start-date",
    "--start_date",
    type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
    help="first date of deletion requests to process; DOES NOT apply to ping date; "
    "defaults to 14 days before --end-date in UTC",
)
parser.add_argument(
    "-p",
    "--billing-projects",
    "--billing_projects",
    "--billing-project",
    "--billing_project",
    nargs="+",
    default=["moz-fx-data-bq-batch-prod"],
    help="One or more billing projects over which bigquery jobs should be distributed; "
    "if not specified use the bigquery-batch-prod project",
)
parser.add_argument(
    "--source-project",
    "--source_project",
    help="override the project used for deletion request tables",
)
parser.add_argument(
    "--target-project",
    "--target_project",
    help="override the project used for target tables",
)
parser.add_argument(
    "--max-single-dml-bytes",
    "--max_single_dml_bytes",
    default=10 * 2 ** 40,
    type=int,
    help="Maximum number of bytes in a table that should be processed using a single "
    "DML query; tables above this limit will be processed using per-partition "
    "queries; this option prevents queries against large tables from exceeding the "
    "6-hour time limit; defaults to 10 TiB",
)
parser.add_argument(
    "--priority",
    default=bigquery.QueryPriority.INTERACTIVE,
    type=str.upper,
    choices=[bigquery.QueryPriority.BATCH, bigquery.QueryPriority.INTERACTIVE],
    help="Priority for BigQuery query jobs; BATCH priority may significantly slow "
    "down queries if reserved slots are not enabled for the billing project; "
    "INTERACTIVE priority is limited to 100 concurrent queries per project",
)
parser.add_argument(
    "--state-table",
    "--state_table",
    type=str.upper,
    METAVAR="TABLE",
    choices=[bigquery.QueryPriority.BATCH, bigquery.QueryPriority.INTERACTIVE],
    help="Table for recording state; Used to avoid repeating deletes if interrupted; "
    "Create it if it does not exist; By default state is not recorded",
)
add_table_filter_arguments(parser)

WHERE_CLAUSE = """
WHERE
  {target.field} IN (
    SELECT DISTINCT
      {source.field}
    FROM
      `{source.sql_table_id}`
    WHERE
      {source_condition}
  )
  AND {partition_condition}
"""

DELETE_TEMPLATE = f"""
DELETE
  `{{target.sql_table_id}}`
{WHERE_CLAUSE.strip()}
"""

SELECT_TEMPLATE = f"""
CREATE TABLE
  `{{destination.project}}.{{destination.dataset_id}}.{{destination.table_id}}`
PARTITION BY
  {{partition_expr}}
{{clustering}}
OPTIONS (
  expiration_timestamp = '{{expiration_timestamp}}'
)
AS
SELECT
  *
FROM
  `{{target.sql_table_id}}`
{WHERE_CLAUSE.strip()}
  AND {{cluster_condition}}
"""


async def gather_all(futures):
    """Gather results, raise last exception, and log other exceptions."""
    results = await asyncio.gather(*futures, return_exceptions=True)
    exceptions = [e for e in results if isinstance(e, Exception)]
    if exceptions:
        for e in exceptions[:-1]:
            logging.exception(e)
        raise exceptions[-1]
    return results


def run_query(client, query, dry_run, priority):
    """Run a query in bigquery and wait for it to complete."""
    job = client.query(
        query, bigquery.QueryJobConfig(dry_run=dry_run, priority=priority)
    )
    if not dry_run:
        job.result()
    return job


def get_partition_expr(table):
    """Get the SQL expression to use for a table's partitioning field."""
    for field in table.schema:
        if field.name != table.time_partitioning.field:
            continue
        if field.field_type == "TIMESTAMP":
            return f"DATE({field.name})"
        return field.name


def list_partitions(client, target):
    """List the partition ids and corresponding dates in a table."""
    return [
        (partition_id, datetime.strpdate("%Y%m%d", partition_id).date())
        for row in client.query(
            f"SELECT partition_id FROM [{target.sql_table_id}$__PARTITION_SUMMARY__]",
            bigquery.QueryJobConfig(use_legacy_sql=True),
        ).result()
        for partition_id in [row["partition_id"]]
    ]


async def delete_from_partition(
    client_q,
    executor,
    dry_run,
    priority,
    target,
    partition_id,
    clustering_fields,
    **template_kwargs,
):
    """Process deletion requests for a single partition of a target table."""
    client = client_q.default_client
    template_kwargs["target"] = target
    if not target.cluster_conditions:
        queries = [DELETE_TEMPLATE.format(**template_kwargs)]
    else:
        clustering = f"CLUSTER BY {', '.join(clustering_fields)}"
        expiration_date = datetime.utcnow().date() + timedelta(days=15)
        queries = [
            SELECT_TEMPLATE.format(
                # expire in 15 days because this script may take up to 14 days
                expiration_timestamp=f"{expiration_date} 00:00:00 UTC",
                destination=get_temporary_table(client),
                clustering=(clustering if needs_clustering else ""),
                cluster_condition=cluster_condition,
                **template_kwargs,
            )
            for cluster_condition, needs_clustering in target.cluster_conditions
        ]
    run_tense = "Would run" if dry_run else "Running"
    for query in queries:
        logging.debug(f"{run_tense} query: {query}")
    loop = asyncio.get_running_loop()
    jobs = await gather_all(
        loop.run_in_executor(
            executor, client_q.with_client, run_query, query, dry_run, priority
        )
        for query in queries
    )
    # copy results into place if necessary
    if target.cluster_conditions:
        sources = [
            f"{t.project}.{t.dataset_id}.{t.table_id}"
            for t in [job.destination for job in jobs]
        ]
        dest = f"{target.sql_table_id}${partition_id}"
        # update clustering fields if necessary before copying into place
        if dry_run:
            updates = [
                sources[index]
                for index, (_, needs_clustering) in enumerate(target.cluster_conditions)
                if not needs_clustering
            ]
            logging.info(f"Would update clustering on {len(updates)} temp tables")
            for table in updates:
                logging.debug(f"Would update clustering on {table}")
            logging.info(
                f"Would overwrite {dest} by copying {len(sources)} temp tables"
            )
            logging.debug(f"Would overwrite {dest} by copying {', '.join(sources)}")
            for table in sources:
                logging.debug(f"Would delete {table}")
        else:
            for source in sources:
                table = client.get_table(source)
                if table.clustering_fields != clustering_fields:
                    logging.debug(f"Updating clustering on {source}")
                    table.clustering_fields = clustering_fields
                    client.update_table(table, ["clustering"])
            logging.debug(f"Overwriting {dest} by copying {', '.join(sources)}")
            client.copy_table(
                sources,
                dest,
                bigquery.CopyJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                ),
            ).result()
            for table in sources:
                logging.debug("Deleting {table}")
                client.delete_table(table)
    return sum(job.total_bytes_processed for job in jobs)


async def delete_from_table(
    client_q, target, dry_run, end_date, max_single_dml_bytes, **kwargs
):
    """Process deletion requests for a single target table."""
    client = client_q.default_client
    table = client.get_table(target.sql_table_id)
    partition_expr = get_partition_expr(table)
    bytes_deleted = 0
    bytes_processed = sum(
        await gather_all(
            delete_from_partition(
                client_q=client_q,
                clustering_fields=table.clustering_fields,
                dry_run=dry_run,
                partition_condition=(
                    f"{partition_expr} <= '{end_date}'"
                    if partition_date is None
                    else f"{partition_expr} = '{partition_date}'"
                ),
                partition_id=partition_id,
                target=target,
                **kwargs,
            )
            for partition_id, partition_date in (
                list_partitions(client=client, target=target)
                if table.num_bytes > max_single_dml_bytes or target.cluster_conditions
                else [(None, None)]
            )
            if partition_date is None or partition_date < end_date
        )
    )
    if dry_run:
        logging.info(f"Would scan {bytes_processed} bytes from {target.table}")
    else:
        bytes_deleted = (
            table.num_bytes - client.get_table(target.sql_table_id).num_bytes
        )
        logging.info(
            f"Scanned {bytes_processed} bytes and "
            f"deleted {bytes_deleted} from {target.table}"
        )
    return bytes_processed, bytes_deleted


def main():
    """Process deletion requests."""
    args = parser.parse_args()
    if args.start_date is None:
        args.start_date = args.end_date - timedelta(days=14)
    logging.root.setLevel(args.log_level)
    source_condition = (
        f"DATE(submission_timestamp) >= '{args.start_date}' "
        f"AND DATE(submission_timestamp) < '{args.end_date}'"
    )
    table_filter = get_table_filter(args)
    client_q = ClientQueue(args.billing_projects, args.parallelism)
    # TODO reorganize to support args.status_table
    # tasks = generate nested list of jobs to run
    # status = client.query("""
    # SELECT
    #   ARRAY_AGG(event ORDER BY submission_timestamp DESC)[OFFSET(0)].*
    # FROM status_table AS event
    # GROUP BY task_name
    # """)
    # tasks = join(tasks, status) on task_name
    # distribute tasks:
    #     log and remove completed jobs
    #     log retrying for failed jobs
    #     wait on job instead of resubmitting for active jobs
    with ThreadPoolExecutor(max_workers=args.parallelism) as executor:
        results = asyncio.run(
            gather_all(
                delete_from_table(
                    client_q=client_q,
                    executor=executor,
                    target=replace(
                        target, project=args.target_project or target.project
                    ),
                    source=replace(
                        source, project=args.source_project or source.project
                    ),
                    source_condition=source_condition,
                    dry_run=args.dry_run,
                    priority=args.priority,
                    end_date=args.end_date,
                    max_single_dml_bytes=args.max_single_dml_bytes,
                )
                for target, source in DELETE_TARGETS.items()
                if table_filter(target.table)
            )
        )
    if not results:
        logging.error("No tables selected")
        parser.exit(1)
    bytes_processed, bytes_deleted = map(sum, zip(*results))
    if args.dry_run:
        logging.info(f"Would scan {bytes_processed} in total")
    else:
        logging.info(f"Scanned {bytes_processed} and deleted {bytes_deleted} in total")


if __name__ == "__main__":
    warnings.filterwarnings("ignore", module="google.auth._default")
    main()
