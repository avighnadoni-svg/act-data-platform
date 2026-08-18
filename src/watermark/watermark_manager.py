# src/watermark/watermark_manager.py

from dataclasses import dataclass

from datetime import (
    datetime,
    timedelta,
    timezone,
)

import os
import re

import snowflake.connector


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
# SNOWFLAKE WATERMARK CONFIGURATION
# ============================================================

DEFAULT_SNOWFLAKE_CONNECTION_NAME = (
    "SNOWFLAKE_ACT_DEV"
)

WATERMARK_TABLE = (
    "ACT_DB.CONTROL.WATERMARK"
)


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
# Downstream Snowflake RAW processing deduplicates replayed
# versions.
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

    variable_key is retained only for backward compatibility
    with any existing callers that inspect this dataclass.

    It is NOT used to read or write Airflow Variables.
    """

    study_id: str

    entity_name: str

    variable_key: str

    stored_watermark: str | None

    extraction_watermark: str | None

    overlap_seconds: int


# ============================================================
# CONNECTION NAME
# ============================================================

def _get_connection_name() -> str:
    """
    Resolve the named Snowflake connection.

    Default:

        SNOWFLAKE_ACT_DEV

    Optional override:

        SNOWFLAKE_CONNECTION_NAME
    """

    value = os.getenv(
        "SNOWFLAKE_CONNECTION_NAME",
        DEFAULT_SNOWFLAKE_CONNECTION_NAME,
    ).strip()


    if not value:

        raise ConfigurationError(
            "SNOWFLAKE_CONNECTION_NAME is empty"
        )


    return value


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
# LEGACY VARIABLE KEY
# ============================================================

def _sanitize_key_component(
    value: str,
) -> str:
    """
    Preserve the existing key format for WatermarkState
    backward compatibility only.

    No Airflow Variable API is called anywhere in this module.
    """

    return re.sub(
        r"[^A-Za-z0-9_.\-]",
        "_",
        value,
    )


def _build_variable_key(
    study_id: str,
    entity_name: str,
) -> str:
    """
    Return the old deterministic key string for compatibility.

    IMPORTANT:
        This string is informational only.
        Snowflake CONTROL.WATERMARK is the source of truth.
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
    value: str | datetime,
) -> datetime:
    """
    Parse timestamp and normalize to UTC.

    Supports strings such as:

        2026-08-16T08:10:26Z

        2026-08-16T08:10:26+00:00

    and datetime objects returned by Snowflake.
    """

    if isinstance(
        value,
        datetime,
    ):

        parsed = value


    else:

        if not value:

            raise ValueError(
                "Timestamp cannot be empty"
            )


        timestamp_value = (
            str(
                value
            )
            .strip()
        )


        if timestamp_value.endswith(
            "Z"
        ):

            timestamp_value = (
                timestamp_value[:-1]
                + "+00:00"
            )


        parsed = datetime.fromisoformat(
            timestamp_value
        )


    if parsed.tzinfo is None:

        parsed = parsed.replace(
            tzinfo=timezone.utc
        )


    return parsed.astimezone(
        timezone.utc
    )


# ============================================================
# NORMALIZE TIMESTAMP
# ============================================================

def _normalize_timestamp(
    value: str | datetime,
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

    Single source of truth:

        ACT_DB.CONTROL.WATERMARK

    Airflow Variables are NOT read or written.
    """

    def __init__(
        self,
        connection_name: str | None = None,
    ) -> None:

        self.connection_name = (
            connection_name
            or _get_connection_name()
        )


    # ========================================================
    # OPEN SNOWFLAKE CONNECTION
    # ========================================================

    def _connect(
        self,
    ):
        """
        Open a short-lived Snowflake transaction connection.
        """

        try:

            return snowflake.connector.connect(
                connection_name=
                    self.connection_name,

                application=
                    "ACT_DATA_PLATFORM_WATERMARK",

                autocommit=
                    False,
            )


        except Exception as exc:

            logger.exception(
                (
                    "watermark_snowflake_connection_failed "
                    "connection_name=%s"
                ),
                self.connection_name,
            )


            raise WatermarkReadError(
                (
                    "Unable to connect to Snowflake "
                    "for watermark processing"
                )
            ) from exc


    # ========================================================
    # GET STORED WATERMARK
    # ========================================================

    def get_watermark(
        self,
        study_id: str,
        entity_name: str,
    ) -> str | None:
        """
        Return the last successfully committed Snowflake
        watermark.

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


        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "watermark_read_started "
                "source=SNOWFLAKE "
                "table=%s"
            ),
            clean_study_id,
            entity_name,
            WATERMARK_TABLE,
        )


        conn = None


        try:

            conn = self._connect()


            with conn.cursor() as cur:

                cur.execute(
                    f"""
                    SELECT
                        WATERMARK_VALUE
                    FROM {WATERMARK_TABLE}
                    WHERE STUDY_ID = %s
                      AND ENTITY_NAME = %s
                    ORDER BY UPDATED_AT DESC
                    LIMIT 2
                    """,
                    (
                        clean_study_id,
                        entity_name,
                    ),
                )


                rows = cur.fetchall()


            conn.commit()


        except WatermarkReadError:

            if conn is not None:

                try:
                    conn.rollback()
                except Exception:
                    pass

            raise


        except Exception as exc:

            if conn is not None:

                try:
                    conn.rollback()
                except Exception:
                    pass


            logger.exception(
                (
                    "study_id=%s "
                    "entity=%s "
                    "watermark_read_failed "
                    "source=SNOWFLAKE"
                ),
                clean_study_id,
                entity_name,
            )


            raise WatermarkReadError(
                (
                    "Unable to read Snowflake watermark "
                    f"study_id={clean_study_id} "
                    f"entity={entity_name}"
                )
            ) from exc


        finally:

            if conn is not None:

                conn.close()


        # ====================================================
        # FIRST RUN
        # ====================================================

        if not rows:

            logger.info(
                (
                    "study_id=%s "
                    "entity=%s "
                    "watermark_not_found "
                    "load_type=FULL "
                    "source=SNOWFLAKE"
                ),
                clean_study_id,
                entity_name,
            )


            return None


        # ====================================================
        # DEFENSIVE DUPLICATE CHECK
        # ====================================================

        if len(
            rows
        ) > 1:

            logger.error(
                (
                    "study_id=%s "
                    "entity=%s "
                    "duplicate_watermark_rows_detected"
                ),
                clean_study_id,
                entity_name,
            )


            raise WatermarkReadError(
                (
                    "Multiple Snowflake watermark rows "
                    "exist for the same study/entity "
                    f"study_id={clean_study_id} "
                    f"entity={entity_name}"
                )
            )


        value = rows[0][0]


        # ====================================================
        # VALIDATE STORED VALUE
        # ====================================================

        try:

            normalized = (
                _normalize_timestamp(
                    value
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
                    "Stored Snowflake watermark is invalid "
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
                "load_type=INCREMENTAL "
                "source=SNOWFLAKE"
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
        Snowflake watermark.

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
                "overlap_seconds=%s "
                "source=SNOWFLAKE"
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
        run_id: str | None = None,
    ) -> str:
        """
        Persist a new successful watermark in Snowflake.

        IMPORTANT:

        This method must only be called AFTER:

            extract
            parse
            validate
            normalize
            S3 upload
            S3 verification

        have all completed successfully.

        The watermark can move forward or remain unchanged.
        It can never move backwards.
        """

        clean_study_id = (
            _validate_study_id(
                study_id
            )
        )


        _validate_entity(
            entity_name
        )


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


        conn = None


        try:

            conn = self._connect()


            with conn.cursor() as cur:

                # ============================================
                # READ CURRENT VALUE IN THE SAME TRANSACTION
                # ============================================

                cur.execute(
                    f"""
                    SELECT
                        WATERMARK_VALUE
                    FROM {WATERMARK_TABLE}
                    WHERE STUDY_ID = %s
                      AND ENTITY_NAME = %s
                    ORDER BY UPDATED_AT DESC
                    LIMIT 2
                    """,
                    (
                        clean_study_id,
                        entity_name,
                    ),
                )


                rows = cur.fetchall()


                if len(
                    rows
                ) > 1:

                    raise WatermarkUpdateError(
                        (
                            "Multiple Snowflake watermark rows "
                            "exist for the same study/entity "
                            f"study_id={clean_study_id} "
                            f"entity={entity_name}"
                        )
                    )


                current_watermark = None


                if rows:

                    current_watermark = (
                        _normalize_timestamp(
                            rows[0][0]
                        )
                    )


                    current_datetime = (
                        _parse_timestamp(
                            current_watermark
                        )
                    )


                    # ========================================
                    # PREVENT WATERMARK REGRESSION
                    # ========================================

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


                    # ========================================
                    # SAME WATERMARK
                    # ========================================

                    if new_datetime == current_datetime:

                        # ------------------------------------
                        # Watermark value is unchanged, but a
                        # successful run can still refresh the
                        # operational run metadata.
                        # ------------------------------------

                        if run_id:

                            cur.execute(
                                f"""
                                UPDATE {WATERMARK_TABLE}
                                SET
                                    LAST_SUCCESSFUL_RUN_ID = %s,
                                    UPDATED_AT =
                                        CURRENT_TIMESTAMP()
                                WHERE STUDY_ID = %s
                                  AND ENTITY_NAME = %s
                                """,
                                (
                                    run_id,
                                    clean_study_id,
                                    entity_name,
                                ),
                            )


                        conn.commit()


                        logger.info(
                            (
                                "study_id=%s "
                                "entity=%s "
                                "watermark_unchanged "
                                "watermark=%s "
                                "run_id=%s "
                                "source=SNOWFLAKE"
                            ),
                            clean_study_id,
                            entity_name,
                            current_watermark,
                            run_id,
                        )


                        return current_watermark


                # ============================================
                # MERGE WATERMARK
                # ============================================

                cur.execute(
                    f"""
                    MERGE INTO {WATERMARK_TABLE} AS T
                    USING
                    (
                        SELECT
                            %s::VARCHAR AS STUDY_ID,
                            %s::VARCHAR AS ENTITY_NAME,
                            %s::TIMESTAMP_TZ AS WATERMARK_VALUE,
                            %s::VARCHAR AS LAST_SUCCESSFUL_RUN_ID
                    ) AS S

                    ON  T.STUDY_ID = S.STUDY_ID
                    AND T.ENTITY_NAME = S.ENTITY_NAME

                    WHEN MATCHED
                         AND S.WATERMARK_VALUE
                             >= T.WATERMARK_VALUE
                    THEN UPDATE SET
                        T.WATERMARK_VALUE =
                            S.WATERMARK_VALUE,

                        T.LAST_SUCCESSFUL_RUN_ID =
                            COALESCE(
                                S.LAST_SUCCESSFUL_RUN_ID,
                                T.LAST_SUCCESSFUL_RUN_ID
                            ),

                        T.UPDATED_AT =
                            CURRENT_TIMESTAMP()

                    WHEN NOT MATCHED THEN
                    INSERT
                    (
                        STUDY_ID,
                        ENTITY_NAME,
                        WATERMARK_VALUE,
                        LAST_SUCCESSFUL_RUN_ID,
                        CREATED_AT,
                        UPDATED_AT
                    )
                    VALUES
                    (
                        S.STUDY_ID,
                        S.ENTITY_NAME,
                        S.WATERMARK_VALUE,
                        S.LAST_SUCCESSFUL_RUN_ID,
                        CURRENT_TIMESTAMP(),
                        CURRENT_TIMESTAMP()
                    )
                    """,
                    (
                        clean_study_id,
                        entity_name,
                        normalized_new,
                        run_id,
                    ),
                )


            conn.commit()


        except WatermarkUpdateError:

            if conn is not None:

                try:
                    conn.rollback()
                except Exception:
                    pass

            raise


        except Exception as exc:

            if conn is not None:

                try:
                    conn.rollback()
                except Exception:
                    pass


            logger.exception(
                (
                    "study_id=%s "
                    "entity=%s "
                    "watermark_update_failed "
                    "source=SNOWFLAKE "
                    "new_watermark=%s"
                ),
                clean_study_id,
                entity_name,
                normalized_new,
            )


            raise WatermarkUpdateError(
                (
                    "Unable to update Snowflake watermark "
                    f"study_id={clean_study_id} "
                    f"entity={entity_name}"
                )
            ) from exc


        finally:

            if conn is not None:

                conn.close()


        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "watermark_update_completed "
                "old_watermark=%s "
                "new_watermark=%s "
                "run_id=%s "
                "source=SNOWFLAKE"
            ),
            clean_study_id,
            entity_name,
            current_watermark,
            normalized_new,
            run_id,
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

        Snowflake is the source of truth.

        variable_key is returned only to preserve the existing
        WatermarkState interface.
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


        if stored_watermark is None:

            extraction_watermark = None


        else:

            if overlap_seconds < 0:

                raise ConfigurationError(
                    (
                        "overlap_seconds cannot "
                        "be negative"
                    )
                )


            extraction_watermark = (
                (
                    _parse_timestamp(
                        stored_watermark
                    )
                    - timedelta(
                        seconds=overlap_seconds
                    )
                )
                .isoformat()
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
        Delete the Snowflake watermark.

        Mainly useful during development/testing.

        Deleting the row causes the next load to behave
        as an initial FULL load.

        This does NOT touch legacy Airflow Variables.
        """

        clean_study_id = (
            _validate_study_id(
                study_id
            )
        )


        _validate_entity(
            entity_name
        )


        conn = None


        try:

            conn = self._connect()


            with conn.cursor() as cur:

                cur.execute(
                    f"""
                    DELETE FROM {WATERMARK_TABLE}
                    WHERE STUDY_ID = %s
                      AND ENTITY_NAME = %s
                    """,
                    (
                        clean_study_id,
                        entity_name,
                    ),
                )


            conn.commit()


        except Exception as exc:

            if conn is not None:

                try:
                    conn.rollback()
                except Exception:
                    pass


            logger.exception(
                (
                    "study_id=%s "
                    "entity=%s "
                    "watermark_delete_failed "
                    "source=SNOWFLAKE"
                ),
                clean_study_id,
                entity_name,
            )


            raise WatermarkUpdateError(
                (
                    "Unable to delete Snowflake watermark "
                    f"study_id={clean_study_id} "
                    f"entity={entity_name}"
                )
            ) from exc


        finally:

            if conn is not None:

                conn.close()


        logger.info(
            (
                "study_id=%s "
                "entity=%s "
                "watermark_deleted "
                "source=SNOWFLAKE"
            ),
            clean_study_id,
            entity_name,
        )
