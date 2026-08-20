# src/api/extract_all.py

from dataclasses import asdict, dataclass
from typing import Any

from config.endpoints import DEFAULT_PAGE_SIZE, ENDPOINTS

from src.api.rave_client import RaveAPIClient

from src.common.exceptions import (
    ACTPipelineError,
    ConfigurationError,
    DataValidationError,
)

from src.common.logging_config import get_logger

from src.parsers.parser_factory import parse_response

from src.processing.normalizer import normalize_dataframe

from src.processing.validator import validate_records

from src.snowflake.control_audit import ControlAuditClient

from src.snowflake.stage_loader import SnowflakeStageLoader

from src.storage.storage_factory import get_storage_backend

from src.watermark.watermark_manager import WatermarkManager


logger = get_logger(__name__)


MAX_PAGES_PER_EXTRACTION = 10_000


# ============================================================
# INGESTION RESULT
# ============================================================

@dataclass
class IngestionResult:
    """
    Result for one study/entity ingestion.

    Current local flow:

        Rave API
            ->
        validate / normalize
            ->
        StorageBackend
            ->
        Local filesystem
            ->
        Snowflake internal stage
    """

    study_id: str
    entity_name: str
    load_type: str

    run_id: str
    load_date: str

    pages_processed: int
    records_received: int

    stored: bool
    staged: bool

    storage_backend: str

    storage_path: str | None
    storage_uri: str | None

    stage_uri: str | None
    stage_upload_status: str | None

    checksum: str | None

    previous_watermark: str | None
    extraction_watermark: str | None
    new_watermark: str | None

    watermark_committed: bool


    @property
    def uploaded(self) -> bool:
        """
        Temporary compatibility property for the existing DAG.

        For the current pipeline:

            uploaded = successfully staged in Snowflake.
        """

        return self.staged


    def to_dict(self) -> dict[str, Any]:
        """
        Return XCom-friendly metadata.
        """

        result = asdict(self)

        result["uploaded"] = (
            self.uploaded
        )

        return result


# ============================================================
# INPUT VALIDATION
# ============================================================

def _validate_inputs(
    study_id: str,
    entity_name: str,
    run_id: str,
    load_date: str,
    page_size: int,
) -> None:

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
            "page_size must be between 1 and 100"
        )


# ============================================================
# STUDY-SCOPE VALIDATION
# ============================================================

def _validate_record_study_scope(
    study_id: str,
    entity_name: str,
    records: list[dict],
) -> None:
    """
    Ensure every source record belongs to the requested study.
    """

    if not records:
        return

    unexpected_studies = set()

    for record in records:

        record_study_id = (
            record.get("study_id")
        )

        if record_study_id is None:
            raise DataValidationError(
                (
                    "study_id missing from extracted "
                    f"record entity={entity_name}"
                )
            )

        normalized_study_id = (
            str(record_study_id)
            .strip()
            .upper()
        )

        if (
            normalized_study_id
            != study_id
        ):

            unexpected_studies.add(
                normalized_study_id
            )

    if unexpected_studies:

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
                unexpected_studies
            ),
        )

        raise DataValidationError(
            (
                "Source API returned records "
                "for the wrong study. "
                f"requested={study_id}, "
                f"unexpected="
                f"{sorted(unexpected_studies)}"
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
) -> tuple[list[dict], int]:
    """
    Extract all pages for one study/entity.
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

    while True:

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

        response = client.get_page(
            entity_name=entity_name,
            updated_since=
                extraction_watermark,
            offset=offset,
            limit=page_size,
            extra_params={
                "study_id": study_id
            },
        )

        page_records = parse_response(
            entity_name=entity_name,
            raw_text=response.text,
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

        if page_record_count == 0:
            break

        all_records.extend(
            page_records
        )

        if (
            page_record_count
            < page_size
        ):
            break

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
        len(all_records),
    )

    return (
        all_records,
        pages_processed,
    )


# ============================================================
# AUDIT FAILURE
# ============================================================

def _mark_entity_audit_failed(
    control_audit_client: ControlAuditClient,
    entity_load_audit_id: str | None,
    error: Exception,
) -> None:
    """
    Best-effort failure audit update.
    """

    if not entity_load_audit_id:
        return

    try:

        control_audit_client.finish_entity_load(
            entity_load_audit_id=
                entity_load_audit_id,

            status=
                "FAILED",

            error_message=
                str(error),
        )

    except Exception:

        logger.exception(
            (
                "entity_audit_failure_update_failed "
                "entity_load_audit_id=%s"
            ),
            entity_load_audit_id,
        )


# ============================================================
# AUDIT SUCCESS
# ============================================================

def _finish_entity_audit(
    control_audit_client: ControlAuditClient,
    entity_load_audit_id: str,
    status: str,
    source_row_count: int,
    storage_row_count: int,
    source_watermark_to: str | None,
    storage_uri: str | None,
    file_checksum: str | None,
) -> None:
    """
    Complete ENTITY_LOAD_AUDIT.

    STORAGE_URI represents the final successfully verified
    storage handoff used by downstream processing.

    Current local architecture:

        @ACT_DB.RAW.ACT_RAW_STAGE/...
    """

    control_audit_client.finish_entity_load(
        entity_load_audit_id=
            entity_load_audit_id,

        status=
            status,

        source_row_count=
            source_row_count,

        storage_row_count=
            storage_row_count,

        snowflake_row_count=
            None,

        source_watermark_to=
            source_watermark_to,

        storage_uri=
            storage_uri,

        file_checksum=
            file_checksum,

        error_message=
            None,
    )


# ============================================================
# MAIN INGESTION
# ============================================================

def ingest_study_entity(
    study_id: str,
    entity_name: str,
    run_id: str,
    load_date: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    commit_watermark: bool = True,
    dag_id: str | None = "act_rave_ingestion",
    task_id: str | None = None,
    map_index: int | None = None,
    attempt_number: int | None = None,
) -> IngestionResult:
    """
    Ingest exactly one study/entity.

    Process
    -------
        1. Read existing watermark
        2. Calculate extraction watermark
        3. Start CONTROL audit
        4. Extract source records
        5. Parse and validate
        6. Normalize
        7. Write through StorageBackend
        8. Stage file into Snowflake
        9. Verify stage upload
       10. Commit watermark
       11. Complete CONTROL audit

    Watermark rule
    --------------
    The normal watermark advances only after the normalized
    file has successfully reached and been verified in the
    Snowflake internal RAW stage.
    """

    study_id = (
        study_id
        .strip()
        .upper()
    )

    entity_name = (
        entity_name
        .strip()
        .lower()
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

    watermark_manager = (
        WatermarkManager()
    )

    control_audit_client = (
        ControlAuditClient()
    )

    storage_backend = (
        get_storage_backend()
    )

    stage_loader = (
        SnowflakeStageLoader()
    )

    entity_load_audit_id: (
        str | None
    ) = None

    previous_watermark: (
        str | None
    ) = None

    extraction_watermark: (
        str | None
    ) = None

    load_type = "UNKNOWN"

    try:

        # ====================================================
        # 1. PREVIOUS WATERMARK
        # ====================================================

        previous_watermark = (
            watermark_manager.get_watermark(
                study_id=study_id,
                entity_name=entity_name,
            )
        )

        load_type = (
            "FULL"
            if previous_watermark is None
            else "INCREMENTAL"
        )

        # ====================================================
        # 2. EXTRACTION WATERMARK
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
                "extraction_watermark=%s "
                "storage_backend=%s"
            ),
            study_id,
            entity_name,
            load_type,
            previous_watermark,
            extraction_watermark,
            storage_backend.backend_name,
        )

        # ====================================================
        # 3. START AUDIT
        # ====================================================

        entity_load_audit_id = (
            control_audit_client
            .start_entity_load(
                dag_id=dag_id,
                dag_run_id=run_id,
                task_id=task_id,
                map_index=map_index,
                attempt_number=
                    attempt_number,
                study_id=study_id,
                entity_name=
                    entity_name,
                load_type=load_type,
                source_watermark_from=
                    extraction_watermark,
            )
        )

        # ====================================================
        # 4. EXTRACT ALL PAGES
        # ====================================================

        with RaveAPIClient() as api_client:

            (
                records,
                pages_processed,
            ) = _extract_all_pages(
                client=api_client,
                study_id=study_id,
                entity_name=entity_name,
                extraction_watermark=
                    extraction_watermark,
                page_size=page_size,
            )

        # ====================================================
        # 5. STUDY SCOPE
        # ====================================================

        _validate_record_study_scope(
            study_id=study_id,
            entity_name=entity_name,
            records=records,
        )

        # ====================================================
        # 6. VALIDATE
        # ====================================================

        validation = validate_records(
            entity_name=entity_name,
            records=records,
        )

        # ====================================================
        # NO NEW DATA
        # ====================================================

        if validation.record_count == 0:

            _finish_entity_audit(
                control_audit_client=
                    control_audit_client,

                entity_load_audit_id=
                    entity_load_audit_id,

                status=
                    "NO_NEW_DATA",

                source_row_count=
                    0,

                storage_row_count=
                    0,

                source_watermark_to=
                    previous_watermark,

                storage_uri=
                    None,

                file_checksum=
                    None,
            )

            logger.info(
                (
                    "study_id=%s "
                    "entity=%s "
                    "ingestion_completed "
                    "status=NO_NEW_DATA"
                ),
                study_id,
                entity_name,
            )

            return IngestionResult(
                study_id=study_id,
                entity_name=entity_name,
                load_type=load_type,
                run_id=run_id,
                load_date=load_date,
                pages_processed=
                    pages_processed,
                records_received=0,
                stored=False,
                staged=False,
                storage_backend=
                    storage_backend.backend_name,
                storage_path=None,
                storage_uri=None,
                stage_uri=None,
                stage_upload_status=None,
                checksum=None,
                previous_watermark=
                    previous_watermark,
                extraction_watermark=
                    extraction_watermark,
                new_watermark=
                    previous_watermark,
                watermark_committed=False,
            )

        # ====================================================
        # 7. NORMALIZE
        # ====================================================

        normalized = normalize_dataframe(
            entity_name=entity_name,
            df=validation.dataframe,
            run_id=run_id,
        )

        storage_row_count = len(
            normalized.dataframe
        )

        # ====================================================
        # 8. WRITE STORAGE
        # ====================================================

        storage_result = (
            storage_backend
            .write_dataframe(
                entity_name=entity_name,
                study_id=study_id,
                dataframe=
                    normalized.dataframe,
                run_id=run_id,
                load_date=load_date,
            )
        )

        if not storage_result.stored:

            raise DataValidationError(
                (
                    "Non-empty ingestion did not "
                    "produce a storage object "
                    f"study_id={study_id} "
                    f"entity={entity_name}"
                )
            )

        if (
            storage_result.record_count
            != storage_row_count
        ):

            raise DataValidationError(
                (
                    "Storage record-count mismatch. "
                    f"expected="
                    f"{storage_row_count}, "
                    f"actual="
                    f"{storage_result.record_count}"
                )
            )

        if not storage_result.storage_path:

            raise DataValidationError(
                (
                    "Current Snowflake stage handoff "
                    "requires a local storage_path. "
                    f"backend="
                    f"{storage_result.storage_backend}"
                )
            )

        # ====================================================
        # 9. SNOWFLAKE INTERNAL STAGE
        # ====================================================

        stage_result = (
            stage_loader.upload_file(
                local_file_path=
                    storage_result.storage_path,

                entity_name=
                    entity_name,

                study_id=
                    study_id,

                load_date=
                    load_date,

                run_id=
                    run_id,
            )
        )

        if (
            stage_result.upload_status
            not in {
                "UPLOADED",
                "SKIPPED",
            }
        ):

            raise DataValidationError(
                (
                    "Snowflake stage upload was "
                    "not successful. "
                    f"status="
                    f"{stage_result.upload_status}"
                )
            )

        # ====================================================
        # 10. NEW WATERMARK
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
        # 11. COMMIT WATERMARK
        # ====================================================
        #
        # Reached only after:
        #
        # storage write            OK
        # checksum verification    OK
        # Snowflake PUT            OK
        # Snowflake LIST verify    OK
        # ====================================================

        watermark_committed = False

        if commit_watermark:

            new_watermark = (
                watermark_manager
                .update_watermark(
                    study_id=study_id,
                    entity_name=
                        entity_name,
                    new_watermark=
                        new_watermark,
                    run_id=run_id,
                )
            )

            watermark_committed = True

        else:

            logger.warning(
                (
                    "study_id=%s "
                    "entity=%s "
                    "watermark_commit_skipped "
                    "candidate_watermark=%s"
                ),
                study_id,
                entity_name,
                new_watermark,
            )

        # ====================================================
        # 12. FINISH AUDIT
        # ====================================================

        _finish_entity_audit(
            control_audit_client=
                control_audit_client,

            entity_load_audit_id=
                entity_load_audit_id,

            status=
                "SUCCESS",

            source_row_count=
                validation.record_count,

            storage_row_count=
                storage_row_count,

            source_watermark_to=
                new_watermark,

            # Final verified downstream handoff.
            storage_uri=
                stage_result.stage_uri,

            file_checksum=
                storage_result.checksum,
        )

        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "ingestion_completed "
                "status=SUCCESS "
                "load_type=%s "
                "records=%s "
                "pages=%s "
                "storage_backend=%s "
                "storage_uri=%s "
                "stage_uri=%s "
                "stage_status=%s "
                "previous_watermark=%s "
                "new_watermark=%s "
                "watermark_committed=%s "
                "entity_load_audit_id=%s"
            ),
            study_id,
            entity_name,
            load_type,
            validation.record_count,
            pages_processed,
            storage_result.storage_backend,
            storage_result.storage_uri,
            stage_result.stage_uri,
            stage_result.upload_status,
            previous_watermark,
            new_watermark,
            watermark_committed,
            entity_load_audit_id,
        )

        return IngestionResult(
            study_id=study_id,
            entity_name=entity_name,
            load_type=load_type,
            run_id=run_id,
            load_date=load_date,
            pages_processed=
                pages_processed,
            records_received=
                validation.record_count,
            stored=True,
            staged=True,
            storage_backend=
                storage_result.storage_backend,
            storage_path=
                storage_result.storage_path,
            storage_uri=
                storage_result.storage_uri,
            stage_uri=
                stage_result.stage_uri,
            stage_upload_status=
                stage_result.upload_status,
            checksum=
                storage_result.checksum,
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

    except ACTPipelineError as exc:

        _mark_entity_audit_failed(
            control_audit_client=
                control_audit_client,

            entity_load_audit_id=
                entity_load_audit_id,

            error=
                exc,
        )

        logger.exception(
            (
                "study_id=%s "
                "entity=%s "
                "ingestion_failed "
                "run_id=%s "
                "entity_load_audit_id=%s"
            ),
            study_id,
            entity_name,
            run_id,
            entity_load_audit_id,
        )

        raise

    # ========================================================
    # UNEXPECTED ERROR
    # ========================================================

    except Exception as exc:

        _mark_entity_audit_failed(
            control_audit_client=
                control_audit_client,

            entity_load_audit_id=
                entity_load_audit_id,

            error=
                exc,
        )

        logger.exception(
            (
                "study_id=%s "
                "entity=%s "
                "unexpected_ingestion_failure "
                "run_id=%s "
                "entity_load_audit_id=%s"
            ),
            study_id,
            entity_name,
            run_id,
            entity_load_audit_id,
        )

        raise