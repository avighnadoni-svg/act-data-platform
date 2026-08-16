# src/parsers/parser_factory.py

from config.endpoints import ENDPOINTS

from src.common.exceptions import (
    ConfigurationError,
    UnsupportedFormatError,
)

from src.common.logging_config import get_logger

from src.parsers.json_parser import (
    parse_json_response,
)

from src.parsers.xml_parser import (
    parse_xml_response,
)

from src.parsers.csv_parser import (
    parse_csv_response,
)


logger = get_logger(__name__)


def parse_response(
    entity_name: str,
    raw_text: str,
) -> list[dict]:

    if entity_name not in ENDPOINTS:

        raise ConfigurationError(
            (
                "Endpoint configuration "
                f"missing for {entity_name}"
            )
        )

    source_format = (
        ENDPOINTS[
            entity_name
        ]["format"]
        .lower()
    )

    logger.info(
        (
            "entity=%s "
            "parser_selected "
            "format=%s"
        ),
        entity_name,
        source_format,
    )

    if source_format == "json":

        return parse_json_response(
            entity_name,
            raw_text,
        )

    if source_format == "xml":

        return parse_xml_response(
            entity_name,
            raw_text,
        )

    if source_format == "csv":

        return parse_csv_response(
            entity_name,
            raw_text,
        )

    raise UnsupportedFormatError(
        (
            f"Unsupported source format="
            f"{source_format} "
            f"for entity={entity_name}"
        )
    )