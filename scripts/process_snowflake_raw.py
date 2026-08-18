#!/usr/bin/env python3
"""
Manual smoke test for ACT Snowflake RAW processing.

Examples
--------

Process one entity:

    python scripts/process_snowflake_raw.py \
        --entity adverse_event

Process all eight entities sequentially:

    python scripts/process_snowflake_raw.py \
        --all

This script executes the EXISTING tested Option 3 SQL files.

It does not create new RAW SQL logic.
"""

from __future__ import annotations

import argparse

from pathlib import Path

import sys


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)


if str(PROJECT_ROOT) not in sys.path:

    sys.path.insert(
        0,
        str(PROJECT_ROOT),
    )


from src.snowflake.raw_processor import (
    RAW_PROCESS_ORDER,
    RAW_PROCESS_SQL_FILES,
    SnowflakeRawProcessor,
)


def _print_result(
    result,
) -> None:
    """
    Print one compact execution result.
    """

    print()
    print("SNOWFLAKE RAW PROCESS RESULT")
    print("============================")
    print(
        f"entity_name: "
        f"{result.entity_name}"
    )
    print(
        f"sql_file: "
        f"{result.sql_file}"
    )
    print(
        f"status: "
        f"{result.status}"
    )
    print(
        f"statements_executed: "
        f"{result.statements_executed}"
    )

    if result.query_ids:

        print(
            f"first_query_id: "
            f"{result.query_ids[0]}"
        )

        print(
            f"last_query_id: "
            f"{result.query_ids[-1]}"
        )


def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Execute existing ACT Snowflake "
            "RAW Option 3 processing SQL."
        )
    )


    mode = parser.add_mutually_exclusive_group(
        required=True
    )


    mode.add_argument(
        "--entity",
        choices=sorted(
            RAW_PROCESS_SQL_FILES.keys()
        ),
        help=(
            "Process one RAW entity"
        ),
    )


    mode.add_argument(
        "--all",
        action="store_true",
        help=(
            "Process all eight RAW entities "
            "sequentially"
        ),
    )


    args = parser.parse_args()


    processor = (
        SnowflakeRawProcessor()
    )


    if args.all:

        for entity_name in (
            RAW_PROCESS_ORDER
        ):

            result = (
                processor.process_entity(
                    entity_name
                )
            )

            _print_result(
                result
            )


    else:

        result = (
            processor.process_entity(
                args.entity
            )
        )

        _print_result(
            result
        )


if __name__ == "__main__":

    main()
