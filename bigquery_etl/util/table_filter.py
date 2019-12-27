"""TODO."""

from argparse import ArgumentParser, Namespace
from functools import partial
from typing import Callable, List
import fnmatch
import re
import logging


def add_table_filter_arguments(
    parser: ArgumentParser, example: str = "telemetry_stable.main_v*"
):
    """TODO."""
    format_ = f"pass names or globs like {example!r}"
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-o",
        "--only",
        nargs="+",
        dest="only_tables",
        help=f"Process only the given tables; {format_}",
    )
    group.add_argument(
        "-x",
        "--except",
        nargs="+",
        dest="except_tables",
        help=f"Process all tables except for the given tables; {format_}",
    )


def compile_glob_patterns(patterns: List[str]) -> re.Pattern:
    """TODO."""
    return re.compile("|".join(fnmatch.translate(pattern) for pattern in patterns))


def glob_predicate(table: str, pattern: re.Pattern, arg: str) -> bool:
    """TODO."""
    matched = pattern.match(table) is not None
    if arg == "except":
        matched = not matched
    if not matched:
        logging.info(f"Skipping {table} due to --{arg} argument")
    return matched


def get_table_filter(args: Namespace) -> Callable[[str], bool]:
    """TODO."""
    for patterns, arg in [(args.only_tables, "only"), (args.except_tables, "except")]:
        if patterns:
            pattern = compile_glob_patterns(patterns)
            return partial(glob_predicate, pattern=pattern, arg=arg)
    return lambda table: True
