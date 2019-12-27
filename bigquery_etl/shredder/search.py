#!/usr/bin/env python3

"""Search for tables and user ids that may be eligible for self serve deletion."""

from argparse import ArgumentParser
from itertools import chain
import re
import warnings

from google.cloud import bigquery

from .config import DELETE_TARGETS, SHARED_PROD, SOURCES, UNSUPPORTED
from ..util.table_filter import add_table_filter_arguments, get_table_filter


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "-p",
    "--project",
    "--project_id",
    "--project-id",
    default=SHARED_PROD,
    help=f"ID of the project in which to find tables; defaults to {SHARED_PROD}",
)
add_table_filter_arguments(parser)

KNOWN_SUPPORTED_TABLES = {e.table for e in chain(SOURCES, DELETE_TARGETS)}
KNOWN_UNSUPPORTED_IDS = {(target.table, target.field) for target in UNSUPPORTED}

# TODO this should move to config.py
ID_PATTERN = re.compile(r"(\b|_)id")
IGNORE_PATTERN = re.compile(
    "|".join(
        [
            "activation_id",
            "addon_id",
            "application_id",
            "batch_id",
            "bucket_id",
            "(de)?bug_id",
            "build_id",
            "campaign_id",
            "changeset_id",
            "crash_id",
            "device_id",
            "distribution_id",
            "document_id",
            "error_id",
            "experiment_id",
            "extension_id",
            "encryption_key_id",
            "insert_id",
            "message_id",
            "model_id",
            "network_id",
            "page_id",
            "partner_id",
            "product_id",
            "run_id",
            "setter_id",
            "survey_id",
            "sample_id",
            "(sub)?session_id",
            "subsys(tem)?_id",
            "thread_id",
            "tile_id",
            "vendor_id",
            "id_bucket",
            r"active_experiment\.id",
            r"theme\.id",
            r"tiles\[]\.id",
            r"spoc_fills\[]\.id",
            r"devices\[]\.id",
            r"application\.id",
            r"environment\.id",
            # these prefixes need to be evaluated
            "enrollment_id",  # for experiments
            "flow_id",
            "intent_id",
            "requestee_id",
        ]
    )
)
DATASET_PATTERN = re.compile(".*_(stable|derived)")


def find_id_fields(fields, prefix=""):
    """Recursively locate potential ids in fields."""
    for field in fields:
        name = prefix + field.name
        if field.field_type == "RECORD":
            prefix += field.name + ("[]" if field.mode == "REPEATED" else "") + "."
            yield from find_id_fields(field.fields, prefix)
        elif ID_PATTERN.search(name) and not IGNORE_PATTERN.search(name):
            yield name


def find_target_tables(project, table_filter):
    """Search for potential new ids and new tables with ids."""
    client = bigquery.Client()
    for dataset in client.list_datasets(project):
        if not DATASET_PATTERN.match(dataset.dataset_id):
            continue
        for table_ref in client.list_tables(dataset.reference):
            table = f"{table_ref.dataset_id}.{table_ref.table_id}"
            if not table_filter(table) or table in KNOWN_SUPPORTED_TABLES:
                continue
            for field in find_id_fields(client.get_table(table_ref).schema):
                result = table, field
                if result not in KNOWN_UNSUPPORTED_IDS:
                    yield result


def main():
    """Print results of find_target_tables."""
    args = parser.parse_args()
    table_filter = get_table_filter(args)
    for table, field in find_target_tables(args.project, table_filter):
        print(f"table={table!r}, field={field!r}")


if __name__ == "__main__":
    warnings.filterwarnings("ignore", module="google.auth._default")
    main()
