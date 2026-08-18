#!/usr/bin/env python3
"""
One-time migration of ACT watermarks from Airflow Variables to Snowflake.

Old source:
    Airflow Variable
    act_watermark__<STUDY_ID>__<ENTITY_NAME>

New single source of truth:
    ACT_DB.CONTROL.WATERMARK

Migration approach:
1. Discover current studies from RAW_STUDY_CURRENT.
2. Loop through configured ACT entities.
3. Read the existing Airflow Variable.
4. MERGE the value into ACT_DB.CONTROL.WATERMARK.
5. Validate the Snowflake row after migration.

Important:
- This script does NOT delete Airflow Variables.
- Keep the old variables temporarily until Snowflake migration is verified.
- After the replacement WatermarkManager is installed, Airflow Variables are
  ignored for watermark processing.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import re
import sys

import snowflake.connector


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.endpoints import ENDPOINTS


DEFAULT_CONNECTION_NAME = "SNOWFLAKE_ACT_DEV"

WATERMARK_TABLE = "ACT_DB.CONTROL.WATERMARK"


def _get_airflow_variable_class():
    """
    Import the Airflow 3 Task SDK Variable class.
    """

    try:
        from airflow.sdk import Variable
        return Variable

    except ImportError as exc:
        raise RuntimeError(
            "airflow.sdk.Variable is unavailable"
        ) from exc


def _connection_name() -> str:
    """
    Resolve Snowflake named connection.
    """

    value = os.getenv(
        "SNOWFLAKE_CONNECTION_NAME",
        DEFAULT_CONNECTION_NAME,
    ).strip()

    if not value:
        raise RuntimeError(
            "SNOWFLAKE_CONNECTION_NAME is empty"
        )

    return value


def _sanitize_key_component(
    value: str,
) -> str:
    """
    Preserve the exact legacy Airflow key-building rule.
    """

    return re.sub(
        r"[^A-Za-z0-9_.\-]",
        "_",
        value,
    )


def _build_legacy_variable_key(
    study_id: str,
    entity_name: str,
) -> str:
    """
    Build the existing Airflow watermark variable key.
    """

    return (
        "act_watermark__"
        f"{_sanitize_key_component(study_id)}__"
        f"{_sanitize_key_component(entity_name)}"
    )


def _parse_timestamp(
    value: str,
) -> datetime:
    """
    Parse ISO timestamp and normalize to UTC.
    """

    timestamp_value = str(value).strip()

    if not timestamp_value:
        raise ValueError(
            "Timestamp cannot be empty"
        )

    if timestamp_value.endswith("Z"):
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


def _get_studies(
    conn,
) -> list[str]:
    """
    Discover study IDs from the completed RAW current layer.
    """

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT STUDY_ID
            FROM ACT_DB.RAW.RAW_STUDY_CURRENT
            WHERE STUDY_ID IS NOT NULL
            ORDER BY STUDY_ID
            """
        )

        return [
            str(row[0]).strip().upper()
            for row in cur.fetchall()
        ]


def _merge_watermark(
    conn,
    *,
    study_id: str,
    entity_name: str,
    watermark_value: datetime,
) -> None:
    """
    Upsert one migrated watermark.
    """

    sql = f"""
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

        WHEN MATCHED THEN
        UPDATE SET
            T.WATERMARK_VALUE = S.WATERMARK_VALUE,
            T.LAST_SUCCESSFUL_RUN_ID =
                S.LAST_SUCCESSFUL_RUN_ID,
            T.UPDATED_AT = CURRENT_TIMESTAMP()

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
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                study_id,
                entity_name,
                watermark_value,
                None,
            ),
        )


def main() -> None:
    Variable = _get_airflow_variable_class()

    connection_name = _connection_name()

    conn = snowflake.connector.connect(
        connection_name=connection_name,
        application="ACT_WATERMARK_MIGRATION",
        autocommit=False,
    )

    migrated = []
    missing = []

    try:
        studies = _get_studies(
            conn
        )

        if not studies:
            raise RuntimeError(
                "No studies found in "
                "ACT_DB.RAW.RAW_STUDY_CURRENT"
            )

        for study_id in studies:

            for entity_name in ENDPOINTS.keys():

                variable_key = (
                    _build_legacy_variable_key(
                        study_id,
                        entity_name,
                    )
                )

                value = Variable.get(
                    variable_key,
                    default=None,
                )

                if value is None:
                    missing.append(
                        (
                            study_id,
                            entity_name,
                            variable_key,
                        )
                    )
                    continue

                parsed = _parse_timestamp(
                    str(value)
                )

                _merge_watermark(
                    conn,
                    study_id=study_id,
                    entity_name=entity_name,
                    watermark_value=parsed,
                )

                migrated.append(
                    (
                        study_id,
                        entity_name,
                        parsed.isoformat(),
                    )
                )

        conn.commit()

        print()
        print("AIRFLOW -> SNOWFLAKE WATERMARK MIGRATION")
        print("========================================")

        for (
            study_id,
            entity_name,
            watermark_value,
        ) in migrated:

            print(
                f"MIGRATED  "
                f"{study_id:<10} "
                f"{entity_name:<22} "
                f"{watermark_value}"
            )

        for (
            study_id,
            entity_name,
            variable_key,
        ) in missing:

            print(
                f"MISSING   "
                f"{study_id:<10} "
                f"{entity_name:<22} "
                f"{variable_key}"
            )

        print()
        print(
            f"migrated_count: {len(migrated)}"
        )
        print(
            f"missing_count: {len(missing)}"
        )

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    STUDY_ID,
                    ENTITY_NAME,
                    WATERMARK_VALUE,
                    LAST_SUCCESSFUL_RUN_ID
                FROM {WATERMARK_TABLE}
                ORDER BY
                    STUDY_ID,
                    ENTITY_NAME
                """
            )

            rows = cur.fetchall()

        print()
        print("SNOWFLAKE WATERMARK TABLE")
        print("=========================")

        for row in rows:
            print(
                f"{row[0]:<10} "
                f"{row[1]:<22} "
                f"{row[2]} "
                f"{row[3]}"
            )

        print()
        print(
            f"snowflake_row_count: {len(rows)}"
        )

        expected = (
            len(studies)
            * len(ENDPOINTS)
        )

        print(
            f"expected_row_count: {expected}"
        )

        if missing:
            print()
            print(
                "WARNING: Some legacy Airflow watermarks "
                "were missing. Do NOT switch the "
                "WatermarkManager until you review them."
            )

        elif len(rows) < expected:
            raise RuntimeError(
                "Snowflake WATERMARK row count is "
                "lower than expected"
            )

        else:
            print()
            print("migration_validation: PASS")

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()
