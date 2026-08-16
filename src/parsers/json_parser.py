# src/parsers/json_parser.py

import json
from typing import Any

from src.common.exceptions import (
    JSONParsingError,
)

from src.common.logging_config import (
    get_logger,
)


logger = get_logger(__name__)


# ============================================================
# STUDY
# ============================================================

def _parse_study(
    payload: dict[str, Any],
) -> list[dict]:
    """
    Flatten Study nested JSON.
    """

    output = []

    records = payload.get(
        "data",
        [],
    )


    for item in records:

        study = item.get(
            "study",
            {},
        )

        details = study.get(
            "details",
            {},
        )

        enrollment = study.get(
            "enrollment",
            {},
        )

        audit = item.get(
            "audit",
            {},
        )


        output.append(
            {

                "study_id":
                    study.get(
                        "identifier"
                    ),

                "study_name":
                    details.get(
                        "name"
                    ),

                "phase":
                    details.get(
                        "phase"
                    ),

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


# ============================================================
# SUBJECT
# ============================================================

def _parse_subject(
    payload: dict[str, Any],
) -> list[dict]:
    """
    Flatten Subject nested JSON.
    """

    output = []


    response = payload.get(
        "response",
        {},
    )

    records = response.get(
        "subjects",
        [],
    )


    for item in records:

        subject = item.get(
            "subject",
            {},
        )

        trial_context = subject.get(
            "trial_context",
            {},
        )

        study = trial_context.get(
            "study",
            {},
        )

        site = trial_context.get(
            "site",
            {},
        )

        demographics = subject.get(
            "demographics",
            {},
        )

        clinical_status = subject.get(
            "clinical_status",
            {},
        )

        enrollment = subject.get(
            "enrollment",
            {},
        )

        audit = item.get(
            "audit",
            {},
        )

        timestamps = audit.get(
            "timestamps",
            {},
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


# ============================================================
# LAB RESULT
# ============================================================

def _parse_lab_result(
    payload: dict[str, Any],
) -> list[dict]:
    """
    Flatten Lab Result nested JSON.

    Lab Result now contains study_id.

    Example:

        lab_result
            study
                study_id
            subject
                subject_id
            test
                result
    """

    output = []


    extract = payload.get(
        "laboratory_extract",
        {},
    )


    records = extract.get(
        "results",
        [],
    )


    for item in records:

        lab = item.get(
            "lab_result",
            {},
        )


        # ----------------------------------------------------
        # STUDY
        # ----------------------------------------------------

        study = lab.get(
            "study",
            {},
        )


        # ----------------------------------------------------
        # SUBJECT
        # ----------------------------------------------------

        subject = lab.get(
            "subject",
            {},
        )


        # ----------------------------------------------------
        # TEST
        # ----------------------------------------------------

        test = lab.get(
            "test",
            {},
        )


        # ----------------------------------------------------
        # RESULT
        # ----------------------------------------------------

        result = test.get(
            "result",
            {},
        )


        # ----------------------------------------------------
        # REFERENCE RANGE
        # ----------------------------------------------------

        reference_range = result.get(
            "reference_range",
            {},
        )


        # ----------------------------------------------------
        # INTERPRETATION
        # ----------------------------------------------------

        interpretation = result.get(
            "interpretation",
            {},
        )


        # ----------------------------------------------------
        # METADATA
        # ----------------------------------------------------

        metadata = item.get(
            "metadata",
            {},
        )


        audit = metadata.get(
            "audit",
            {},
        )


        # ----------------------------------------------------
        # FLAT RECORD
        # ----------------------------------------------------

        output.append(
            {

                "lab_id":
                    lab.get(
                        "identifier"
                    ),


                # ============================================
                # NEW
                # ============================================

                "study_id":
                    study.get(
                        "study_id"
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
# PUBLIC JSON PARSER
# ============================================================

def parse_json_response(
    entity_name: str,
    raw_text: str,
) -> list[dict]:
    """
    Parse JSON response based on ACT entity.

    Supported:

        study
        subject
        lab_result
    """

    logger.info(
        (
            "entity=%s "
            "json_parsing_started"
        ),
        entity_name,
    )


    try:

        # ====================================================
        # JSON DECODE
        # ====================================================

        payload = json.loads(
            raw_text
        )


        # ====================================================
        # STUDY
        # ====================================================

        if entity_name == "study":

            records = _parse_study(
                payload
            )


        # ====================================================
        # SUBJECT
        # ====================================================

        elif entity_name == "subject":

            records = _parse_subject(
                payload
            )


        # ====================================================
        # LAB RESULT
        # ====================================================

        elif entity_name == "lab_result":

            records = _parse_lab_result(
                payload
            )


        # ====================================================
        # UNKNOWN
        # ====================================================

        else:

            raise JSONParsingError(
                (
                    "No JSON parser configured "
                    f"for entity={entity_name}"
                )
            )


        # ====================================================
        # SUCCESS
        # ====================================================

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


    # ========================================================
    # KNOWN PARSER EXCEPTION
    # ========================================================

    except JSONParsingError:

        raise


    # ========================================================
    # INVALID JSON
    # ========================================================

    except json.JSONDecodeError as exc:

        logger.exception(
            (
                "entity=%s "
                "invalid_json"
            ),
            entity_name,
        )


        raise JSONParsingError(
            (
                "Invalid JSON received "
                f"for entity={entity_name}"
            )
        ) from exc


    # ========================================================
    # OTHER ERROR
    # ========================================================

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