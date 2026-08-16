# src/api/extract_all.py

from dataclasses import (
    asdict,
    dataclass,
)

from typing import Any


from config.endpoints import (
    DEFAULT_PAGE_SIZE,
    ENDPOINTS,
)

from src.api.rave_client import (
    RaveAPIClient,
)

from src.aws.s3_client import (
    ACTS3Client,
)

from src.common.exceptions import (
    ACTPipelineError,
    ConfigurationError,
    DataValidationError,
)

from src.common.logging_config import (
    get_logger,
)

from src.parsers.parser_factory import (
    parse_response,
)

from src.processing.normalizer import (
    normalize_dataframe,
)

from src.processing.validator import (
    validate_records,
)

from src.watermark.watermark_manager import (
    WatermarkManager,
)


logger = get_logger(__name__)


# ============================================================
# SAFETY LIMIT
# ============================================================
#
# Prevent an accidental infinite pagination loop.
#
# 10,000 pages * 100 records
# = 1,000,000 records per entity/study/run.
#
# This is only a defensive limit for the lab.
# ============================================================

MAX_PAGES_PER_EXTRACTION = 10_000


# ============================================================
# INGESTION RESULT
# ============================================================

@dataclass
class IngestionResult:
    """
    Small metadata object returned after one
    study + entity ingestion.

    This is intentionally small because later
    Airflow XCom should carry metadata only,
    not the actual dataset.
    """

    study_id: str

    entity_name: str

    load_type: str

    run_id: str

    load_date: str

    pages_processed: int

    records_received: int

    uploaded: bool

    s3_uri: str | None

    checksum: str | None

    previous_watermark: str | None

    extraction_watermark: str | None

    new_watermark: str | None

    watermark_committed: bool


    def to_dict(
        self,
    ) -> dict[str, Any]:
        """
        Return JSON/XCom-friendly dictionary.
        """

        return asdict(
            self
        )


# ============================================================
# CONFIGURATION VALIDATION
# ============================================================

def _validate_inputs(
    study_id: str,
    entity_name: str,
    run_id: str,
    load_date: str,
    page_size: int,
) -> None:
    """
    Validate ingestion parameters.
    """

    if not study_id:

        raise ConfigurationError(
            "study_id cannot be empty"
        )


    if entity_name not in ENDPOINTS:

        raise ConfigurationError(
            f"Unknown entity={entity_name}"
        )


    if not run_id:

        raise ConfigurationError(
            "run_id cannot be empty"
        )


    if not load_date:

        raise ConfigurationError(
            "load_date cannot be empty"
        )


    if (
        page_size < 1
        or page_size > 100
    ):

        raise ConfigurationError(
            (
                "page_size must be between "
                "1 and 100"
            )
        )


# ============================================================
# STUDY SCOPE VALIDATION
# ============================================================

def _validate_record_study_scope(
    study_id: str,
    entity_name: str,
    records: list[dict],
) -> None:
    """
    Ensure every extracted record belongs to
    the requested study.

    Example:

        Request:
            study_id=ONC101

        Response must NOT contain:
            ONC102

    This catches a source/API filtering problem
    before the data reaches S3.
    """

    if not records:

        return


    wrong_studies = set()


    for record in records:

        record_study_id = (
            record.get(
                "study_id"
            )
        )


        # ----------------------------------------------------
        # Missing study_id will later fail required-field
        # validation, but we fail here with clearer context.
        # ----------------------------------------------------

        if record_study_id is None:

            raise DataValidationError(
                (
                    "study_id missing from extracted "
                    f"record entity={entity_name}"
                )
            )


        record_study_id = (
            str(
                record_study_id
            )
            .strip()
            .upper()
        )


        if record_study_id != study_id:

            wrong_studies.add(
                record_study_id
            )


    if wrong_studies:

        logger.error(
            (
                "study_scope_validation_failed "
                "requested_study=%s "
                "entity=%s "
                "unexpected_studies=%s"
            ),
            study_id,
            entity_name,
            sorted(
                wrong_studies
            ),
        )


        raise DataValidationError(
            (
                "Source API returned records "
                "for the wrong study. "
                f"requested={study_id}, "
                f"unexpected="
                f"{sorted(wrong_studies)}"
            )
        )


# ============================================================
# PAGINATED EXTRACTION
# ============================================================

def _extract_all_pages(
    client: RaveAPIClient,
    study_id: str,
    entity_name: str,
    extraction_watermark: str | None,
    page_size: int,
) -> tuple[
    list[dict],
    int,
]:
    """
    Extract and parse every page for one:

        study
        +
        entity
        +
        watermark

    Returns:

        records
        pages_processed
    """

    all_records: list[dict] = []

    offset = 0

    pages_processed = 0


    logger.info(
        (
            "study_id=%s "
            "entity=%s "
            "paginated_extraction_started "
            "watermark=%s "
            "page_size=%s"
        ),
        study_id,
        entity_name,
        extraction_watermark,
        page_size,
    )


    # ========================================================
    # PAGE LOOP
    # ========================================================

    while True:

        # ----------------------------------------------------
        # DEFENSIVE PAGE LIMIT
        # ----------------------------------------------------

        if (
            pages_processed
            >= MAX_PAGES_PER_EXTRACTION
        ):

            raise DataValidationError(
                (
                    "Maximum page limit exceeded "
                    f"study_id={study_id} "
                    f"entity={entity_name} "
                    f"limit="
                    f"{MAX_PAGES_PER_EXTRACTION}"
                )
            )


        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "page_request_started "
                "offset=%s "
                "limit=%s"
            ),
            study_id,
            entity_name,
            offset,
            page_size,
        )


        # ====================================================
        # API CALL
        # ====================================================

        response = client.get_page(

            entity_name=
                entity_name,

            updated_since=
                extraction_watermark,

            offset=
                offset,

            limit=
                page_size,

            extra_params={
                "study_id":
                    study_id
            },
        )


        # ====================================================
        # PARSE PAGE
        # ====================================================

        page_records = (
            parse_response(
                entity_name=
                    entity_name,

                raw_text=
                    response.text,
            )
        )


        page_record_count = len(
            page_records
        )


        pages_processed += 1


        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "page_processed "
                "page_number=%s "
                "offset=%s "
                "records=%s"
            ),
            study_id,
            entity_name,
            pages_processed,
            offset,
            page_record_count,
        )


        # ====================================================
        # NO RECORDS
        # ====================================================
        #
        # This happens:
        #
        # - no new incremental data
        # - or after a final page containing exactly
        #   page_size rows
        # ====================================================

        if page_record_count == 0:

            break


        # ====================================================
        # APPEND
        # ====================================================

        all_records.extend(
            page_records
        )


        # ====================================================
        # LAST PAGE
        # ====================================================
        #
        # Example:
        #
        # page_size = 100
        # API returns 37
        #
        # Therefore there cannot be another page.
        # ====================================================

        if page_record_count < page_size:

            break


        # ====================================================
        # NEXT PAGE
        # ====================================================

        offset += page_size


    logger.info(
        (
            "study_id=%s "
            "entity=%s "
            "paginated_extraction_completed "
            "pages=%s "
            "total_records=%s"
        ),
        study_id,
        entity_name,
        pages_processed,
        len(
            all_records
        ),
    )


    return (
        all_records,
        pages_processed,
    )


# ============================================================
# MAIN INGESTION FUNCTION
# ============================================================

def ingest_study_entity(
    study_id: str,
    entity_name: str,
    run_id: str,
    load_date: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    commit_watermark: bool = True,
) -> IngestionResult:
    """
    Complete ACT ingestion for exactly:

        ONE study
        +
        ONE entity

    Example:

        ONC101
        +
        adverse_event


    PROCESS
    -------

    1. Read previous watermark

    2. Calculate extraction watermark

    3. Call source API with:

        study_id
        updated_since
        offset
        limit

    4. Parse all pages

    5. Validate

    6. Normalize

    7. Upload CSV to S3

    8. Verify S3 object

    9. ONLY THEN commit new watermark


    IMPORTANT
    ---------

    commit_watermark=False is useful for manual
    testing outside an Airflow task.

    Production Airflow execution should use:

        commit_watermark=True
    """

    # ========================================================
    # NORMALIZE INPUT
    # ========================================================

    study_id = (
        study_id
        .strip()
        .upper()
    )


    _validate_inputs(
        study_id=study_id,
        entity_name=entity_name,
        run_id=run_id,
        load_date=load_date,
        page_size=page_size,
    )


    logger.info(
        (
            "study_id=%s "
            "entity=%s "
            "ingestion_started "
            "run_id=%s "
            "load_date=%s "
            "commit_watermark=%s"
        ),
        study_id,
        entity_name,
        run_id,
        load_date,
        commit_watermark,
    )


    # ========================================================
    # COMPONENTS
    # ========================================================

    watermark_manager = (
        WatermarkManager()
    )


    try:

        # ====================================================
        # 1. READ STORED WATERMARK
        # ====================================================

        previous_watermark = (
            watermark_manager.get_watermark(
                study_id=study_id,
                entity_name=entity_name,
            )
        )


        # ====================================================
        # LOAD TYPE
        # ====================================================

        load_type = (
            "FULL"
            if previous_watermark is None
            else "INCREMENTAL"
        )


        # ====================================================
        # 2. CALCULATE EXTRACTION WATERMARK
        # ====================================================

        extraction_watermark = (
            watermark_manager
            .get_extraction_watermark(
                study_id=study_id,
                entity_name=entity_name,
            )
        )


        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "load_initialized "
                "load_type=%s "
                "previous_watermark=%s "
                "extraction_watermark=%s"
            ),
            study_id,
            entity_name,
            load_type,
            previous_watermark,
            extraction_watermark,
        )


        # ====================================================
        # 3. EXTRACT ALL API PAGES
        # ====================================================

        with RaveAPIClient() as api_client:

            (
                records,
                pages_processed,
            ) = _extract_all_pages(

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


        # ====================================================
        # 4. CHECK STUDY SCOPE
        # ====================================================

        _validate_record_study_scope(
            study_id=study_id,
            entity_name=entity_name,
            records=records,
        )


        # ====================================================
        # 5. VALIDATE
        # ====================================================

        validation = (
            validate_records(
                entity_name=
                    entity_name,

                records=
                    records,
            )
        )


        # ====================================================
        # NO NEW RECORDS
        # ====================================================

        if validation.record_count == 0:

            logger.info(
                (
                    "study_id=%s "
                    "entity=%s "
                    "ingestion_completed "
                    "status=NO_NEW_DATA "
                    "load_type=%s "
                    "previous_watermark=%s"
                ),
                study_id,
                entity_name,
                load_type,
                previous_watermark,
            )


            return IngestionResult(

                study_id=
                    study_id,

                entity_name=
                    entity_name,

                load_type=
                    load_type,

                run_id=
                    run_id,

                load_date=
                    load_date,

                pages_processed=
                    pages_processed,

                records_received=
                    0,

                uploaded=
                    False,

                s3_uri=
                    None,

                checksum=
                    None,

                previous_watermark=
                    previous_watermark,

                extraction_watermark=
                    extraction_watermark,

                new_watermark=
                    previous_watermark,

                watermark_committed=
                    False,
            )


        # ====================================================
        # 6. NORMALIZE
        # ====================================================

        normalized = (
            normalize_dataframe(

                entity_name=
                    entity_name,

                df=
                    validation.dataframe,

                run_id=
                    run_id,
            )
        )


        # ====================================================
        # 7. S3 UPLOAD
        # ====================================================

        s3_client = (
            ACTS3Client()
        )


        upload_result = (
            s3_client.upload_dataframe(

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


        # ====================================================
        # DEFENSIVE CHECK
        # ====================================================
        #
        # Non-empty data must result in an upload.
        # ====================================================

        if not upload_result.uploaded:

            raise DataValidationError(
                (
                    "Non-empty ingestion did not "
                    "produce an S3 object "
                    f"study_id={study_id} "
                    f"entity={entity_name}"
                )
            )


        # ====================================================
        # 8. NEW WATERMARK
        # ====================================================

        new_watermark = (
            validation.max_watermark
        )


        if not new_watermark:

            raise DataValidationError(
                (
                    "Unable to determine new watermark "
                    f"study_id={study_id} "
                    f"entity={entity_name}"
                )
            )


        # ====================================================
        # 9. COMMIT WATERMARK
        # ====================================================
        #
        # CRITICAL RULE:
        #
        # We reach this point only after:
        #
        # S3 put_object
        # AND
        # S3 head_object verification
        #
        # succeeded.
        # ====================================================

        watermark_committed = False


        if commit_watermark:

            committed_watermark = (
                watermark_manager
                .update_watermark(

                    study_id=
                        study_id,

                    entity_name=
                        entity_name,

                    new_watermark=
                        new_watermark,
                )
            )


            new_watermark = (
                committed_watermark
            )


            watermark_committed = True


        else:

            logger.warning(
                (
                    "study_id=%s "
                    "entity=%s "
                    "watermark_commit_skipped "
                    "reason=commit_watermark_false "
                    "candidate_watermark=%s"
                ),
                study_id,
                entity_name,
                new_watermark,
            )


        # ====================================================
        # SUCCESS
        # ====================================================

        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "ingestion_completed "
                "status=SUCCESS "
                "load_type=%s "
                "records=%s "
                "pages=%s "
                "s3_uri=%s "
                "previous_watermark=%s "
                "new_watermark=%s "
                "watermark_committed=%s"
            ),
            study_id,
            entity_name,
            load_type,
            validation.record_count,
            pages_processed,
            upload_result.s3_uri,
            previous_watermark,
            new_watermark,
            watermark_committed,
        )


        return IngestionResult(

            study_id=
                study_id,

            entity_name=
                entity_name,

            load_type=
                load_type,

            run_id=
                run_id,

            load_date=
                load_date,

            pages_processed=
                pages_processed,

            records_received=
                validation.record_count,

            uploaded=
                upload_result.uploaded,

            s3_uri=
                upload_result.s3_uri,

            checksum=
                upload_result.checksum,

            previous_watermark=
                previous_watermark,

            extraction_watermark=
                extraction_watermark,

            new_watermark=
                new_watermark,

            watermark_committed=
                watermark_committed,
        )


    # ========================================================
    # KNOWN PIPELINE ERROR
    # ========================================================

    except ACTPipelineError:

        logger.exception(
            (
                "study_id=%s "
                "entity=%s "
                "ingestion_failed "
                "run_id=%s"
            ),
            study_id,
            entity_name,
            run_id,
        )

        raise


    # ========================================================
    # UNEXPECTED ERROR
    # ========================================================

    except Exception:

        logger.exception(
            (
                "study_id=%s "
                "entity=%s "
                "unexpected_ingestion_failure "
                "run_id=%s"
            ),
            study_id,
            entity_name,
            run_id,
        )

        raise