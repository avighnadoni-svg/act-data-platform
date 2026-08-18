#!/usr/bin/env python3
"""
Manual ACT study/entity reprocessing utility.

Purpose
-------
Reprocess historical source records without changing the normal Snowflake
incremental watermark.

This is useful when the client corrects an older Rave record but does NOT
change its UPDATED_AT value.

Example
-------
python scripts/reprocess_study_entity.py \
    --study-id ONC102 \
    --entity adverse_event \
    --reprocess-from 2026-08-16T17:34:09+00:00

Important
---------
The current mock Rave API supports only updated_since, not an upper-bound
updated_before parameter. Therefore this utility reprocesses records FROM the
requested timestamp onward.

Snowflake is the single source of truth for the normal watermark:

    ACT_DB.CONTROL.WATERMARK

This utility READS the normal watermark before and after reprocessing for audit
evidence, but it never calls update_watermark() or delete_watermark().

Every manual reprocess is recorded in:

    ACT_DB.CONTROL.REPROCESS_AUDIT
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.endpoints import (
    DEFAULT_PAGE_SIZE,
    ENDPOINTS,
)

from src.api.extract_all import (
    _extract_all_pages,
    _validate_record_study_scope,
)

from src.api.rave_client import (
    RaveAPIClient,
)

from src.aws.s3_client import (
    ACTS3Client,
)

from src.common.exceptions import (
    ConfigurationError,
    DataValidationError,
)

from src.common.logging_config import (
    get_logger,
)

from src.processing.normalizer import (
    normalize_dataframe,
)

from src.processing.validator import (
    validate_records,
)

from src.snowflake.control_audit import (
    ControlAuditClient,
)

from src.watermark.watermark_manager import (
    WatermarkManager,
)


logger = get_logger(__name__)


# ============================================================
# TIMESTAMP PARSING
# ============================================================

def _parse_iso_timestamp(
    value: str,
) -> datetime:
    """
    Parse an ISO-8601 timestamp and normalize it to UTC.
    """

    normalized = value.strip()

    if normalized.endswith("Z"):
        normalized = (
            normalized[:-1]
            + "+00:00"
        )

    try:
        parsed = datetime.fromisoformat(
            normalized
        )

    except ValueError as exc:
        raise ConfigurationError(
            (
                "Invalid --reprocess-from timestamp. "
                "Use ISO-8601, for example "
                "2026-08-16T17:34:10+00:00"
            )
        ) from exc


    if parsed.tzinfo is None:
        parsed = parsed.replace(
            tzinfo=timezone.utc
        )


    return parsed.astimezone(
        timezone.utc
    )


# ============================================================
# INCLUSIVE REPROCESS LOWER BOUND
# ============================================================

def _build_extraction_watermark(
    reprocess_from: str,
) -> str:
    """
    The mock source API uses strict:

        updated_at > updated_since

    The user-facing reprocess_from is intended to be inclusive.

    Subtract one microsecond so a source row whose UPDATED_AT is exactly equal
    to reprocess_from is included.
    """

    parsed = _parse_iso_timestamp(
        reprocess_from
    )


    extraction_start = (
        parsed
        - timedelta(
            microseconds=1
        )
    )


    return extraction_start.isoformat()


# ============================================================
# REPROCESS RUN ID
# ============================================================

def _build_run_id(
    study_id: str,
    entity_name: str,
) -> str:
    """
    Build a unique manual reprocess run identifier.
    """

    now = datetime.now(
        timezone.utc
    )


    stamp = now.strftime(
        "%Y%m%dT%H%M%S%fZ"
    )


    return (
        f"reprocess__"
        f"{study_id}__"
        f"{entity_name}__"
        f"{stamp}"
    )


# ============================================================
# REQUESTED BY
# ============================================================

def _requested_by() -> str:
    """
    Resolve a useful operator identity for REPROCESS_AUDIT.

    In GitHub Codespaces this normally resolves from USER.
    """

    return (
        os.getenv("GITHUB_USER")
        or os.getenv("USER")
        or "MANUAL_OPERATOR"
    )


# ============================================================
# BEST-EFFORT WATERMARK READ
# ============================================================

def _read_watermark_best_effort(
    watermark_manager: WatermarkManager,
    study_id: str,
    entity_name: str,
) -> str | None:
    """
    Read the Snowflake normal watermark without hiding the
    original reprocess exception if this read itself fails.
    """

    try:
        return watermark_manager.get_watermark(
            study_id=study_id,
            entity_name=entity_name,
        )

    except Exception:
        logger.exception(
            (
                "manual_reprocess_watermark_read_failed "
                "study_id=%s "
                "entity=%s"
            ),
            study_id,
            entity_name,
        )

        return None


# ============================================================
# BEST-EFFORT FAILED AUDIT FINALIZATION
# ============================================================

def _finish_failed_reprocess_audit(
    control_audit_client: ControlAuditClient,
    reprocess_audit_id: str | None,
    watermark_manager: WatermarkManager,
    study_id: str,
    entity_name: str,
    error: Exception,
) -> None:
    """
    Mark an already-started REPROCESS_AUDIT row FAILED.

    Audit-finalization failure must never hide the original
    reprocess exception.
    """

    if not reprocess_audit_id:
        return


    normal_watermark_after = (
        _read_watermark_best_effort(
            watermark_manager=watermark_manager,
            study_id=study_id,
            entity_name=entity_name,
        )
    )


    try:
        control_audit_client.finish_reprocess(
            reprocess_audit_id=
                reprocess_audit_id,

            status=
                "FAILED",

            source_row_count=
                None,

            s3_uri=
                None,

            file_checksum=
                None,

            normal_watermark_after=
                normal_watermark_after,

            error_message=
                str(error),
        )

    except Exception:
        logger.exception(
            (
                "manual_reprocess_audit_failure_update_failed "
                "reprocess_audit_id=%s "
                "study_id=%s "
                "entity=%s"
            ),
            reprocess_audit_id,
            study_id,
            entity_name,
        )


# ============================================================
# MAIN REPROCESS FUNCTION
# ============================================================

def reprocess_study_entity(
    study_id: str,
    entity_name: str,
    reprocess_from: str,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> dict[str, Any]:
    """
    Reprocess one study/entity from a historical timestamp onward.

    Watermark behavior
    ------------------
    - Snowflake CONTROL.WATERMARK is the normal watermark source.
    - The normal watermark is read before reprocessing.
    - The normal watermark is read again after reprocessing.
    - This function never updates or deletes the normal watermark.

    Reprocess audit behavior
    ------------------------
    - REPROCESS_AUDIT starts as RUNNING before source extraction.
    - SUCCESS is recorded when data is uploaded successfully.
    - NO_DATA is recorded when the source returns no matching records.
    - FAILED is recorded when reprocessing raises an exception.
    """

    study_id = (
        study_id
        .strip()
        .upper()
    )


    entity_name = (
        entity_name
        .strip()
    )


    if not study_id:
        raise ConfigurationError(
            "study_id cannot be empty"
        )


    if entity_name not in ENDPOINTS:
        raise ConfigurationError(
            f"Unknown entity={entity_name}"
        )


    if (
        page_size < 1
        or page_size > 100
    ):
        raise ConfigurationError(
            "page_size must be between 1 and 100"
        )


    # ========================================================
    # VALIDATE / NORMALIZE REQUESTED LOWER BOUND
    # ========================================================

    requested_reprocess_from = (
        _parse_iso_timestamp(
            reprocess_from
        )
        .isoformat()
    )


    extraction_watermark = (
        _build_extraction_watermark(
            requested_reprocess_from
        )
    )


    # ========================================================
    # RUN METADATA
    # ========================================================

    run_id = _build_run_id(
        study_id=study_id,
        entity_name=entity_name,
    )


    load_date = (
        datetime.now(
            timezone.utc
        )
        .date()
        .isoformat()
    )


    requested_by = (
        _requested_by()
    )


    # ========================================================
    # COMPONENTS
    # ========================================================

    watermark_manager = (
        WatermarkManager()
    )


    control_audit_client = (
        ControlAuditClient()
    )


    # ========================================================
    # NORMAL WATERMARK BEFORE REPROCESS
    # ========================================================
    #
    # This is a READ ONLY operation.
    # ========================================================

    normal_watermark_before = (
        watermark_manager.get_watermark(
            study_id=study_id,
            entity_name=entity_name,
        )
    )


    # ========================================================
    # START REPROCESS AUDIT
    # ========================================================
    #
    # reprocess_to remains NULL because the current mock API
    # does not support an upper-bound updated_before filter.
    # ========================================================

    reprocess_audit_id = (
        control_audit_client.start_reprocess(

            dag_run_id=
                run_id,

            study_id=
                study_id,

            entity_name=
                entity_name,

            reprocess_from=
                requested_reprocess_from,

            reprocess_to=
                None,

            requested_by=
                requested_by,

            normal_watermark_before=
                normal_watermark_before,
        )
    )


    logger.warning(
        (
            "manual_reprocess_started "
            "reprocess_audit_id=%s "
            "study_id=%s "
            "entity=%s "
            "requested_reprocess_from=%s "
            "api_updated_since=%s "
            "run_id=%s "
            "requested_by=%s "
            "normal_watermark_before=%s "
            "normal_watermark_will_not_be_modified=true"
        ),
        reprocess_audit_id,
        study_id,
        entity_name,
        requested_reprocess_from,
        extraction_watermark,
        run_id,
        requested_by,
        normal_watermark_before,
    )


    try:

        # ====================================================
        # SOURCE EXTRACTION
        # ====================================================

        with RaveAPIClient() as api_client:

            records, pages_processed = (
                _extract_all_pages(
                    client=
                        api_client,

                    study_id=
                        study_id,

                    entity_name=
                        entity_name,

                    extraction_watermark=
                        extraction_watermark,

                    page_size=
                        page_size,
                )
            )


        # ====================================================
        # STUDY SCOPE VALIDATION
        # ====================================================

        _validate_record_study_scope(
            study_id=
                study_id,

            entity_name=
                entity_name,

            records=
                records,
        )


        # ====================================================
        # DATA VALIDATION
        # ====================================================

        validation = validate_records(
            entity_name=
                entity_name,

            records=
                records,
        )


        # ====================================================
        # NO DATA
        # ====================================================

        if validation.record_count == 0:

            normal_watermark_after = (
                watermark_manager.get_watermark(
                    study_id=
                        study_id,

                    entity_name=
                        entity_name,
                )
            )


            control_audit_client.finish_reprocess(

                reprocess_audit_id=
                    reprocess_audit_id,

                status=
                    "NO_DATA",

                source_row_count=
                    0,

                s3_uri=
                    None,

                file_checksum=
                    None,

                normal_watermark_after=
                    normal_watermark_after,

                error_message=
                    None,
            )


            logger.info(
                (
                    "manual_reprocess_completed "
                    "reprocess_audit_id=%s "
                    "study_id=%s "
                    "entity=%s "
                    "status=NO_DATA "
                    "pages=%s "
                    "normal_watermark_before=%s "
                    "normal_watermark_after=%s "
                    "normal_watermark_modified_by_reprocess=false"
                ),
                reprocess_audit_id,
                study_id,
                entity_name,
                pages_processed,
                normal_watermark_before,
                normal_watermark_after,
            )


            return {
                "reprocess_audit_id":
                    reprocess_audit_id,

                "study_id":
                    study_id,

                "entity_name":
                    entity_name,

                "load_type":
                    "REPROCESS",

                "run_id":
                    run_id,

                "load_date":
                    load_date,

                "records_received":
                    0,

                "pages_processed":
                    pages_processed,

                "uploaded":
                    False,

                "s3_uri":
                    None,

                "checksum":
                    None,

                "reprocess_from":
                    requested_reprocess_from,

                "reprocess_to":
                    None,

                "api_updated_since":
                    extraction_watermark,

                "normal_watermark_before":
                    normal_watermark_before,

                "normal_watermark_after":
                    normal_watermark_after,

                "watermark_committed":
                    False,

                "audit_status":
                    "NO_DATA",
            }


        # ====================================================
        # NORMALIZE
        # ====================================================

        normalized = normalize_dataframe(
            entity_name=
                entity_name,

            df=
                validation.dataframe,

            run_id=
                run_id,
        )


        # ====================================================
        # S3 UPLOAD
        # ====================================================

        upload_result = (
            ACTS3Client()
            .upload_dataframe(
                entity_name=
                    entity_name,

                study_id=
                    study_id,

                df=
                    normalized.dataframe,

                run_id=
                    run_id,

                load_date=
                    load_date,
            )
        )


        if not upload_result.uploaded:
            raise DataValidationError(
                (
                    "Reprocess returned records but "
                    "did not produce an S3 object"
                )
            )


        # ====================================================
        # NORMAL WATERMARK AFTER REPROCESS
        # ========================================================
        #
        # READ ONLY.
        #
        # The reprocess path has made no call to:
        #
        #     update_watermark()
        #     delete_watermark()
        #
        # If the value differs because a normal pipeline ran
        # concurrently, that change is still captured in the
        # audit rather than being overwritten here.
        # ========================================================

        normal_watermark_after = (
            watermark_manager.get_watermark(
                study_id=
                    study_id,

                entity_name=
                    entity_name,
            )
        )


        if (
            normal_watermark_before
            != normal_watermark_after
        ):

            logger.warning(
                (
                    "manual_reprocess_normal_watermark_changed_during_run "
                    "reprocess_audit_id=%s "
                    "study_id=%s "
                    "entity=%s "
                    "before=%s "
                    "after=%s "
                    "reprocess_code_did_not_write_watermark=true"
                ),
                reprocess_audit_id,
                study_id,
                entity_name,
                normal_watermark_before,
                normal_watermark_after,
            )


        # ====================================================
        # COMPLETE REPROCESS AUDIT
        # ====================================================

        control_audit_client.finish_reprocess(

            reprocess_audit_id=
                reprocess_audit_id,

            status=
                "SUCCESS",

            source_row_count=
                validation.record_count,

            s3_uri=
                upload_result.s3_uri,

            file_checksum=
                upload_result.checksum,

            normal_watermark_after=
                normal_watermark_after,

            error_message=
                None,
        )


        logger.info(
            (
                "manual_reprocess_completed "
                "reprocess_audit_id=%s "
                "study_id=%s "
                "entity=%s "
                "status=SUCCESS "
                "records=%s "
                "pages=%s "
                "s3_uri=%s "
                "candidate_source_max_updated_at=%s "
                "normal_watermark_before=%s "
                "normal_watermark_after=%s "
                "normal_watermark_modified_by_reprocess=false"
            ),
            reprocess_audit_id,
            study_id,
            entity_name,
            validation.record_count,
            pages_processed,
            upload_result.s3_uri,
            validation.max_watermark,
            normal_watermark_before,
            normal_watermark_after,
        )


        return {
            "reprocess_audit_id":
                reprocess_audit_id,

            "study_id":
                study_id,

            "entity_name":
                entity_name,

            "load_type":
                "REPROCESS",

            "run_id":
                run_id,

            "load_date":
                load_date,

            "records_received":
                validation.record_count,

            "pages_processed":
                pages_processed,

            "uploaded":
                upload_result.uploaded,

            "s3_uri":
                upload_result.s3_uri,

            "checksum":
                upload_result.checksum,

            "reprocess_from":
                requested_reprocess_from,

            "reprocess_to":
                None,

            "api_updated_since":
                extraction_watermark,

            "source_max_updated_at":
                validation.max_watermark,

            "normal_watermark_before":
                normal_watermark_before,

            "normal_watermark_after":
                normal_watermark_after,

            "watermark_committed":
                False,

            "audit_status":
                "SUCCESS",
        }


    # ========================================================
    # FAILURE
    # ========================================================

    except Exception as exc:

        _finish_failed_reprocess_audit(

            control_audit_client=
                control_audit_client,

            reprocess_audit_id=
                reprocess_audit_id,

            watermark_manager=
                watermark_manager,

            study_id=
                study_id,

            entity_name=
                entity_name,

            error=
                exc,
        )


        logger.exception(
            (
                "manual_reprocess_failed "
                "reprocess_audit_id=%s "
                "study_id=%s "
                "entity=%s "
                "run_id=%s "
                "normal_watermark_modified_by_reprocess=false"
            ),
            reprocess_audit_id,
            study_id,
            entity_name,
            run_id,
        )


        raise


# ============================================================
# CLI
# ============================================================

def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Reprocess one ACT study/entity "
            "without changing its Snowflake "
            "normal watermark."
        )
    )


    parser.add_argument(
        "--study-id",
        required=True,
        help="Study ID, for example ONC102",
    )


    parser.add_argument(
        "--entity",
        required=True,
        choices=sorted(
            ENDPOINTS.keys()
        ),
        help="ACT source entity",
    )


    parser.add_argument(
        "--reprocess-from",
        required=True,
        help=(
            "Inclusive ISO-8601 source UPDATED_AT "
            "timestamp to reprocess from"
        ),
    )


    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
    )


    args = parser.parse_args()


    result = reprocess_study_entity(
        study_id=
            args.study_id,

        entity_name=
            args.entity,

        reprocess_from=
            args.reprocess_from,

        page_size=
            args.page_size,
    )


    print()
    print("REPROCESS RESULT")
    print("================")


    for key, value in result.items():
        print(
            f"{key}: {value}"
        )


if __name__ == "__main__":
    main()
