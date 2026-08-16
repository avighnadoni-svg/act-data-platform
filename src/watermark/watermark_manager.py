# src/watermark/watermark_manager.py

import re

from dataclasses import dataclass

from datetime import (
    datetime,
    timedelta,
    timezone,
)


from config.endpoints import (
    ENDPOINTS,
)

from src.common.exceptions import (
    ConfigurationError,
    WatermarkReadError,
    WatermarkUpdateError,
)

from src.common.logging_config import (
    get_logger,
)


logger = get_logger(__name__)


# ============================================================
# DEFAULT OVERLAP
# ============================================================
#
# Source APIs currently use:
#
#     updated_at > watermark
#
# A small overlap protects us from edge cases around
# timestamp boundaries.
#
# Example:
#
# Stored watermark:
#     10:00:05
#
# Extraction watermark:
#     10:00:00
#
# This may re-read a very small number of records.
# Downstream Snowflake processing will later deduplicate
# by business key + updated_at.
# ============================================================

DEFAULT_WATERMARK_OVERLAP_SECONDS = 5


# ============================================================
# WATERMARK STATE
# ============================================================

@dataclass
class WatermarkState:
    """
    Current watermark information for one
    study + entity combination.
    """

    study_id: str

    entity_name: str

    variable_key: str

    stored_watermark: str | None

    extraction_watermark: str | None

    overlap_seconds: int


# ============================================================
# AIRFLOW VARIABLE
# ============================================================

def _get_airflow_variable_class():
    """
    Lazy import of Airflow Variable.

    This avoids importing Airflow when modules are
    imported for unit tests that do not require it.
    """

    try:

        from airflow.sdk import Variable

        return Variable

    except ImportError as exc:

        raise ConfigurationError(
            (
                "Airflow is not installed or "
                "airflow.sdk.Variable is unavailable"
            )
        ) from exc


# ============================================================
# ENTITY VALIDATION
# ============================================================

def _validate_entity(
    entity_name: str,
) -> None:
    """
    Ensure entity exists in ACT configuration.
    """

    if entity_name not in ENDPOINTS:

        raise ConfigurationError(
            f"Unknown entity={entity_name}"
        )


# ============================================================
# STUDY VALIDATION
# ============================================================

def _validate_study_id(
    study_id: str,
) -> str:
    """
    Validate and clean study identifier.
    """

    if not study_id:

        raise ConfigurationError(
            "study_id cannot be empty"
        )

    cleaned = (
        study_id
        .strip()
        .upper()
    )


    if not cleaned:

        raise ConfigurationError(
            "study_id cannot be empty"
        )


    return cleaned


# ============================================================
# VARIABLE KEY COMPONENT
# ============================================================

def _sanitize_key_component(
    value: str,
) -> str:
    """
    Make values safe for Airflow Variable keys.

    Example:

        ONC-101

    remains:

        ONC-101
    """

    return re.sub(
        r"[^A-Za-z0-9_.\-]",
        "_",
        value,
    )


# ============================================================
# BUILD VARIABLE KEY
# ============================================================

def _build_variable_key(
    study_id: str,
    entity_name: str,
) -> str:
    """
    Build deterministic Airflow Variable key.

    Example:

        act_watermark__ONC101__adverse_event
    """

    clean_study = (
        _sanitize_key_component(
            study_id
        )
    )

    clean_entity = (
        _sanitize_key_component(
            entity_name
        )
    )


    return (
        f"act_watermark__"
        f"{clean_study}__"
        f"{clean_entity}"
    )


# ============================================================
# TIMESTAMP PARSING
# ============================================================

def _parse_timestamp(
    value: str,
) -> datetime:
    """
    Parse ISO timestamp and normalize to UTC.

    Supports:

        2026-08-16T08:10:26Z

    and

        2026-08-16T08:10:26+00:00
    """

    if not value:

        raise ValueError(
            "Timestamp cannot be empty"
        )


    timestamp_value = (
        value.strip()
    )


    # --------------------------------------------------------
    # Python fromisoformat prefers +00:00 instead of Z
    # --------------------------------------------------------

    if timestamp_value.endswith("Z"):

        timestamp_value = (
            timestamp_value[:-1]
            + "+00:00"
        )


    parsed = datetime.fromisoformat(
        timestamp_value
    )


    # --------------------------------------------------------
    # If timezone missing, assume UTC
    # --------------------------------------------------------

    if parsed.tzinfo is None:

        parsed = parsed.replace(
            tzinfo=timezone.utc
        )


    # --------------------------------------------------------
    # Normalize everything to UTC
    # --------------------------------------------------------

    return parsed.astimezone(
        timezone.utc
    )


# ============================================================
# NORMALIZE TIMESTAMP
# ============================================================

def _normalize_timestamp(
    value: str,
) -> str:
    """
    Return normalized UTC ISO timestamp.
    """

    parsed = _parse_timestamp(
        value
    )


    return parsed.isoformat()


# ============================================================
# WATERMARK MANAGER
# ============================================================

class WatermarkManager:
    """
    Manage ACT incremental watermarks.

    Watermark grain:

        study_id + entity_name

    Example:

        ONC101 + adverse_event

    Stored as Airflow Variable:

        act_watermark__ONC101__adverse_event
    """

    # ========================================================
    # GET STORED WATERMARK
    # ========================================================

    def get_watermark(
        self,
        study_id: str,
        entity_name: str,
    ) -> str | None:
        """
        Return the last successfully committed watermark.

        First run:

            None

        Incremental run:

            2026-08-16T08:10:26+00:00
        """

        clean_study_id = (
            _validate_study_id(
                study_id
            )
        )

        _validate_entity(
            entity_name
        )


        variable_key = (
            _build_variable_key(
                clean_study_id,
                entity_name,
            )
        )


        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "watermark_read_started "
                "variable_key=%s"
            ),
            clean_study_id,
            entity_name,
            variable_key,
        )


        try:

            Variable = (
                _get_airflow_variable_class()
            )


            value = Variable.get(
                variable_key,
                default=None,
            )


        except Exception as exc:

            logger.exception(
                (
                    "study_id=%s "
                    "entity=%s "
                    "watermark_read_failed "
                    "variable_key=%s"
                ),
                clean_study_id,
                entity_name,
                variable_key,
            )


            raise WatermarkReadError(
                (
                    "Unable to read watermark "
                    f"study_id={clean_study_id} "
                    f"entity={entity_name}"
                )
            ) from exc


        # ====================================================
        # FIRST RUN
        # ====================================================

        if value is None:

            logger.info(
                (
                    "study_id=%s "
                    "entity=%s "
                    "watermark_not_found "
                    "load_type=FULL"
                ),
                clean_study_id,
                entity_name,
            )


            return None


        # ====================================================
        # VALIDATE STORED VALUE
        # ====================================================

        try:

            normalized = (
                _normalize_timestamp(
                    str(value)
                )
            )


        except (
            ValueError,
            TypeError,
        ) as exc:

            logger.exception(
                (
                    "study_id=%s "
                    "entity=%s "
                    "invalid_stored_watermark "
                    "value=%s"
                ),
                clean_study_id,
                entity_name,
                value,
            )


            raise WatermarkReadError(
                (
                    "Stored watermark is invalid "
                    f"study_id={clean_study_id} "
                    f"entity={entity_name} "
                    f"value={value}"
                )
            ) from exc


        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "watermark_read_completed "
                "watermark=%s "
                "load_type=INCREMENTAL"
            ),
            clean_study_id,
            entity_name,
            normalized,
        )


        return normalized


    # ========================================================
    # GET EXTRACTION WATERMARK
    # ========================================================

    def get_extraction_watermark(
        self,
        study_id: str,
        entity_name: str,
        overlap_seconds: int = (
            DEFAULT_WATERMARK_OVERLAP_SECONDS
        ),
    ) -> str | None:
        """
        Return watermark to send to the source API.

        A small overlap is subtracted from the stored
        watermark.

        Example:

        Stored:
            10:00:05

        overlap:
            5 seconds

        API receives:
            10:00:00

        First run returns None.
        """

        if overlap_seconds < 0:

            raise ConfigurationError(
                (
                    "overlap_seconds cannot "
                    "be negative"
                )
            )


        stored_watermark = (
            self.get_watermark(
                study_id=study_id,
                entity_name=entity_name,
            )
        )


        # ====================================================
        # FIRST RUN
        # ====================================================

        if stored_watermark is None:

            return None


        try:

            stored_datetime = (
                _parse_timestamp(
                    stored_watermark
                )
            )


            extraction_datetime = (
                stored_datetime
                - timedelta(
                    seconds=overlap_seconds
                )
            )


            extraction_watermark = (
                extraction_datetime
                .isoformat()
            )


        except Exception as exc:

            raise WatermarkReadError(
                (
                    "Unable to calculate "
                    "extraction watermark "
                    f"study_id={study_id} "
                    f"entity={entity_name}"
                )
            ) from exc


        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "extraction_watermark_calculated "
                "stored_watermark=%s "
                "extraction_watermark=%s "
                "overlap_seconds=%s"
            ),
            study_id,
            entity_name,
            stored_watermark,
            extraction_watermark,
            overlap_seconds,
        )


        return extraction_watermark


    # ========================================================
    # UPDATE WATERMARK
    # ========================================================

    def update_watermark(
        self,
        study_id: str,
        entity_name: str,
        new_watermark: str,
    ) -> str:
        """
        Persist a new successful watermark.

        IMPORTANT:

        This method must only be called AFTER:

            extract
            parse
            validate
            normalize
            S3 upload
            S3 verification

        have all completed successfully.
        """

        clean_study_id = (
            _validate_study_id(
                study_id
            )
        )


        _validate_entity(
            entity_name
        )


        variable_key = (
            _build_variable_key(
                clean_study_id,
                entity_name,
            )
        )


        # ====================================================
        # VALIDATE NEW WATERMARK
        # ====================================================

        try:

            normalized_new = (
                _normalize_timestamp(
                    new_watermark
                )
            )


            new_datetime = (
                _parse_timestamp(
                    normalized_new
                )
            )


        except (
            ValueError,
            TypeError,
        ) as exc:

            raise WatermarkUpdateError(
                (
                    "Invalid new watermark "
                    f"study_id={clean_study_id} "
                    f"entity={entity_name} "
                    f"value={new_watermark}"
                )
            ) from exc


        # ====================================================
        # READ CURRENT WATERMARK
        # ====================================================

        current_watermark = (
            self.get_watermark(
                study_id=clean_study_id,
                entity_name=entity_name,
            )
        )


        # ====================================================
        # PREVENT WATERMARK GOING BACKWARDS
        # ====================================================

        if current_watermark:

            current_datetime = (
                _parse_timestamp(
                    current_watermark
                )
            )


            if new_datetime < current_datetime:

                logger.error(
                    (
                        "study_id=%s "
                        "entity=%s "
                        "watermark_regression_detected "
                        "current=%s "
                        "new=%s"
                    ),
                    clean_study_id,
                    entity_name,
                    current_watermark,
                    normalized_new,
                )


                raise WatermarkUpdateError(
                    (
                        "Watermark cannot move backwards. "
                        f"current={current_watermark}, "
                        f"new={normalized_new}"
                    )
                )


            # ------------------------------------------------
            # SAME WATERMARK
            # ------------------------------------------------

            if new_datetime == current_datetime:

                logger.info(
                    (
                        "study_id=%s "
                        "entity=%s "
                        "watermark_unchanged "
                        "watermark=%s"
                    ),
                    clean_study_id,
                    entity_name,
                    current_watermark,
                )


                return current_watermark


        # ====================================================
        # WRITE WATERMARK
        # ====================================================

        try:

            Variable = (
                _get_airflow_variable_class()
            )


            Variable.set(
                key=variable_key,
                value=normalized_new,
                description=(
                    "ACT incremental watermark "
                    f"for study={clean_study_id}, "
                    f"entity={entity_name}"
                ),
            )


        except Exception as exc:

            logger.exception(
                (
                    "study_id=%s "
                    "entity=%s "
                    "watermark_update_failed "
                    "variable_key=%s "
                    "new_watermark=%s"
                ),
                clean_study_id,
                entity_name,
                variable_key,
                normalized_new,
            )


            raise WatermarkUpdateError(
                (
                    "Unable to update watermark "
                    f"study_id={clean_study_id} "
                    f"entity={entity_name}"
                )
            ) from exc


        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "watermark_update_completed "
                "old_watermark=%s "
                "new_watermark=%s"
            ),
            clean_study_id,
            entity_name,
            current_watermark,
            normalized_new,
        )


        return normalized_new


    # ========================================================
    # GET STATE
    # ========================================================

    def get_state(
        self,
        study_id: str,
        entity_name: str,
        overlap_seconds: int = (
            DEFAULT_WATERMARK_OVERLAP_SECONDS
        ),
    ) -> WatermarkState:
        """
        Return complete watermark state.
        """

        clean_study_id = (
            _validate_study_id(
                study_id
            )
        )


        _validate_entity(
            entity_name
        )


        variable_key = (
            _build_variable_key(
                clean_study_id,
                entity_name,
            )
        )


        stored_watermark = (
            self.get_watermark(
                study_id=clean_study_id,
                entity_name=entity_name,
            )
        )


        extraction_watermark = (
            self.get_extraction_watermark(
                study_id=clean_study_id,
                entity_name=entity_name,
                overlap_seconds=overlap_seconds,
            )
        )


        return WatermarkState(

            study_id=
                clean_study_id,

            entity_name=
                entity_name,

            variable_key=
                variable_key,

            stored_watermark=
                stored_watermark,

            extraction_watermark=
                extraction_watermark,

            overlap_seconds=
                overlap_seconds,
        )


    # ========================================================
    # DELETE / RESET WATERMARK
    # ========================================================

    def delete_watermark(
        self,
        study_id: str,
        entity_name: str,
    ) -> None:
        """
        Delete watermark.

        Mainly useful during development/testing.

        Deleting the watermark causes the next load
        to behave as an initial FULL load.
        """

        clean_study_id = (
            _validate_study_id(
                study_id
            )
        )


        _validate_entity(
            entity_name
        )


        variable_key = (
            _build_variable_key(
                clean_study_id,
                entity_name,
            )
        )


        try:

            Variable = (
                _get_airflow_variable_class()
            )


            Variable.delete(
                variable_key
            )


        except Exception as exc:

            logger.exception(
                (
                    "study_id=%s "
                    "entity=%s "
                    "watermark_delete_failed "
                    "variable_key=%s"
                ),
                clean_study_id,
                entity_name,
                variable_key,
            )


            raise WatermarkUpdateError(
                (
                    "Unable to delete watermark "
                    f"study_id={clean_study_id} "
                    f"entity={entity_name}"
                )
            ) from exc


        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "watermark_deleted "
                "variable_key=%s"
            ),
            clean_study_id,
            entity_name,
            variable_key,
        )