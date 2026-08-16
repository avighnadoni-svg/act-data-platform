# src/parsers/csv_parser.py

import csv
import io

from src.common.exceptions import CSVParsingError
from src.common.logging_config import get_logger


logger = get_logger(__name__)


def parse_csv_response(
    entity_name: str,
    raw_text: str,
) -> list[dict]:

    logger.info(
        "entity=%s csv_parsing_started",
        entity_name,
    )

    try:

        if not raw_text.strip():

            logger.info(
                (
                    "entity=%s "
                    "csv_response_empty"
                ),
                entity_name,
            )

            return []

        buffer = io.StringIO(
            raw_text
        )

        reader = csv.DictReader(
            buffer
        )

        if not reader.fieldnames:

            raise CSVParsingError(
                (
                    "CSV header missing "
                    f"for entity={entity_name}"
                )
            )

        records = [
            dict(row)
            for row in reader
        ]

        logger.info(
            (
                "entity=%s "
                "csv_parsing_completed "
                "record_count=%s"
            ),
            entity_name,
            len(records),
        )

        return records

    except CSVParsingError:
        raise

    except Exception as exc:

        logger.exception(
            (
                "entity=%s "
                "csv_parsing_failed"
            ),
            entity_name,
        )

        raise CSVParsingError(
            (
                "Failed parsing CSV "
                f"for entity={entity_name}"
            )
        ) from exc