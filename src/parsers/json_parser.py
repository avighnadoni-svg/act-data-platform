# src/parsers/json_parser.py

import json
from typing import Any

from src.common.exceptions import JSONParsingError
from src.common.logging_config import get_logger


logger = get_logger(__name__)


def _parse_study(payload: dict[str, Any]) -> list[dict]:

    output = []

    records = payload.get(
        "data",
        []
    )

    for item in records:

        study = item.get(
            "study",
            {}
        )

        details = study.get(
            "details",
            {}
        )

        enrollment = study.get(
            "enrollment",
            {}
        )

        audit = item.get(
            "audit",
            {}
        )

        output.append(
            {
                "study_id":
                    study.get("identifier"),

                "study_name":
                    details.get("name"),

                "phase":
                    details.get("phase"),

                "target_subjects":
                    enrollment.get(
                        "target_subjects"
                    ),

                "updated_at":
                    audit.get(
                        "last_updated"
                    ),
            }
        )

    return output


def _parse_subject(
    payload: dict[str, Any]
) -> list[dict]:

    output = []

    response = payload.get(
        "response",
        {}
    )

    records = response.get(
        "subjects",
        []
    )

    for item in records:

        subject = item.get(
            "subject",
            {}
        )

        trial_context = subject.get(
            "trial_context",
            {}
        )

        study = trial_context.get(
            "study",
            {}
        )

        site = trial_context.get(
            "site",
            {}
        )

        demographics = subject.get(
            "demographics",
            {}
        )

        clinical_status = subject.get(
            "clinical_status",
            {}
        )

        enrollment = subject.get(
            "enrollment",
            {}
        )

        audit = item.get(
            "audit",
            {}
        )

        timestamps = audit.get(
            "timestamps",
            {}
        )

        output.append(
            {
                "subject_id":
                    subject.get(
                        "identifier"
                    ),

                "study_id":
                    study.get(
                        "study_id"
                    ),

                "site_id":
                    site.get(
                        "site_id"
                    ),

                "gender":
                    demographics.get(
                        "gender"
                    ),

                "age":
                    demographics.get(
                        "age"
                    ),

                "status":
                    clinical_status.get(
                        "status"
                    ),

                "enrollment_date":
                    enrollment.get(
                        "date"
                    ),

                "updated_at":
                    timestamps.get(
                        "updated_at"
                    ),
            }
        )

    return output


def _parse_lab_result(
    payload: dict[str, Any]
) -> list[dict]:

    output = []

    extract = payload.get(
        "laboratory_extract",
        {}
    )

    records = extract.get(
        "results",
        []
    )

    for item in records:

        lab = item.get(
            "lab_result",
            {}
        )

        subject = lab.get(
            "subject",
            {}
        )

        test = lab.get(
            "test",
            {}
        )

        result = test.get(
            "result",
            {}
        )

        reference_range = result.get(
            "reference_range",
            {}
        )

        interpretation = result.get(
            "interpretation",
            {}
        )

        metadata = item.get(
            "metadata",
            {}
        )

        audit = metadata.get(
            "audit",
            {}
        )

        output.append(
            {
                "lab_id":
                    lab.get(
                        "identifier"
                    ),

                "subject_id":
                    subject.get(
                        "subject_id"
                    ),

                "test_name":
                    test.get(
                        "name"
                    ),

                "result_value":
                    result.get(
                        "value"
                    ),

                "normal_low":
                    reference_range.get(
                        "low"
                    ),

                "normal_high":
                    reference_range.get(
                        "high"
                    ),

                "interpretation":
                    interpretation.get(
                        "code"
                    ),

                "abnormal":
                    interpretation.get(
                        "abnormal"
                    ),

                "updated_at":
                    audit.get(
                        "updated_at"
                    ),
            }
        )

    return output


# ============================================================
# PUBLIC FUNCTION
# ============================================================

def parse_json_response(
    entity_name: str,
    raw_text: str,
) -> list[dict]:

    logger.info(
        "entity=%s json_parsing_started",
        entity_name,
    )

    try:

        payload = json.loads(
            raw_text
        )

        if entity_name == "study":

            records = _parse_study(
                payload
            )

        elif entity_name == "subject":

            records = _parse_subject(
                payload
            )

        elif entity_name == "lab_result":

            records = _parse_lab_result(
                payload
            )

        else:

            raise JSONParsingError(
                (
                    "No JSON parser configured "
                    f"for entity={entity_name}"
                )
            )

        logger.info(
            (
                "entity=%s "
                "json_parsing_completed "
                "record_count=%s"
            ),
            entity_name,
            len(records),
        )

        return records

    except JSONParsingError:
        raise

    except json.JSONDecodeError as exc:

        logger.exception(
            "entity=%s invalid_json",
            entity_name,
        )

        raise JSONParsingError(
            (
                "Invalid JSON received "
                f"for entity={entity_name}"
            )
        ) from exc

    except Exception as exc:

        logger.exception(
            (
                "entity=%s "
                "json_parsing_failed"
            ),
            entity_name,
        )

        raise JSONParsingError(
            (
                "Failed parsing JSON "
                f"for entity={entity_name}"
            )
        ) from exc