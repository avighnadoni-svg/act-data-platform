# src/snowflake/reconciliation.py

from __future__ import annotations

import logging
import os
import re

from dataclasses import asdict, dataclass

import snowflake.connector


logger = logging.getLogger(__name__)


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


ENTITY_CONFIG = {
    "study": {
        "raw_table": "RAW_STUDY_CURRENT",
        "target_table": "DIM_STUDY",
        "business_keys": ["STUDY_ID"],
        "technical_key": "STUDY_KEY",
    },
    "site": {
        "raw_table": "RAW_SITE_CURRENT",
        "target_table": "DIM_SITE",
        "business_keys": ["STUDY_ID", "SITE_ID"],
        "technical_key": "SITE_KEY",
    },
    "subject": {
        "raw_table": "RAW_SUBJECT_CURRENT",
        "target_table": "DIM_SUBJECT",
        "business_keys": ["STUDY_ID", "SUBJECT_ID"],
        "technical_key": "SUBJECT_KEY",
    },
    "visit": {
        "raw_table": "RAW_VISIT_CURRENT",
        "target_table": "DIM_VISIT",
        "business_keys": ["STUDY_ID", "VISIT_ID"],
        "technical_key": "VISIT_KEY",
    },
    "adverse_event": {
        "raw_table": "RAW_ADVERSE_EVENT_CURRENT",
        "target_table": "FCT_ADVERSE_EVENT",
        "business_keys": ["STUDY_ID", "AE_ID"],
        "technical_key": "ADVERSE_EVENT_KEY",
    },
    "lab_result": {
        "raw_table": "RAW_LAB_RESULT_CURRENT",
        "target_table": "FCT_LAB_RESULT",
        "business_keys": ["STUDY_ID", "LAB_ID"],
        "technical_key": "LAB_RESULT_KEY",
    },
    "protocol_deviation": {
        "raw_table": "RAW_PROTOCOL_DEVIATION_CURRENT",
        "target_table": "FCT_PROTOCOL_DEVIATION",
        "business_keys": ["STUDY_ID", "DEVIATION_ID"],
        "technical_key": "PROTOCOL_DEVIATION_KEY",
    },
    "data_query": {
        "raw_table": "RAW_DATA_QUERY_CURRENT",
        "target_table": "FCT_DATA_QUERY",
        "business_keys": ["STUDY_ID", "QUERY_ID"],
        "technical_key": "DATA_QUERY_KEY",
    },
}


@dataclass
class StudyCountResult:
    study_id: str
    raw_count: int
    target_count: int
    count_difference: int
    status: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class EntityReconciliationResult:
    entity_name: str
    raw_table: str
    target_table: str
    raw_row_count: int
    target_row_count: int
    row_count_difference: int
    raw_duplicate_groups: int
    target_duplicate_groups: int
    raw_null_business_key_rows: int
    target_null_business_key_rows: int
    target_null_technical_key_rows: int
    failed_study_count: int
    study_results: list[StudyCountResult]
    status: str

    def to_dict(self) -> dict:
        result = asdict(self)
        result["study_results"] = [item.to_dict() for item in self.study_results]
        return result


@dataclass
class PipelineReconciliationResult:
    status: str
    database: str
    raw_schema: str
    mart_schema: str
    entity_count: int
    successful_entity_count: int
    failed_entity_count: int
    entities: list[EntityReconciliationResult]

    def to_dict(self) -> dict:
        result = asdict(self)
        result["entities"] = [item.to_dict() for item in self.entities]
        return result


class PipelineReconciler:
    """
    Reconcile Snowflake RAW current tables against dbt dimensions/facts.

    Validation performed for every ACT entity:

        1. Total RAW row count == dbt target row count.
        2. Row counts match for every STUDY_ID.
        3. No duplicate business-key groups in RAW current.
        4. No duplicate business-key groups in dbt target.
        5. No NULL business keys.
        6. No NULL dbt technical keys.

    The current dbt project uses the standard dbt schema naming pattern:

        profile schema   = DBT_DEV
        marts +schema    = marts
        physical schema  = DBT_DEV_MARTS

    DBT_MART_SCHEMA can override that physical schema when required.
    """

    def __init__(
        self,
        connection_name: str | None = None,
        database: str | None = None,
        raw_schema: str | None = None,
        mart_schema: str | None = None,
    ) -> None:
        self.connection_name = (
            connection_name
            or os.getenv("SNOWFLAKE_CONNECTION_NAME")
            or "SNOWFLAKE_ACT_DEV"
        )

        self.database = (
            database
            or os.getenv("SNOWFLAKE_DATABASE")
            or "ACT_DB"
        )

        self.raw_schema = (
            raw_schema
            or os.getenv("ACT_RAW_SCHEMA")
            or "RAW"
        )

        dbt_base_schema = (
            os.getenv("DBT_DEV_SCHEMA")
            or "DBT_DEV"
        )

        self.mart_schema = (
            mart_schema
            or os.getenv("DBT_MART_SCHEMA")
            or f"{dbt_base_schema}_MARTS"
        )

        for value in (
            self.database,
            self.raw_schema,
            self.mart_schema,
        ):
            self._validate_identifier(value)

    @staticmethod
    def _validate_identifier(value: str) -> str:
        if not _IDENTIFIER_RE.fullmatch(value):
            raise ValueError(f"Unsafe Snowflake identifier: {value!r}")
        return value

    def _qualified_table(self, schema: str, table: str) -> str:
        self._validate_identifier(schema)
        self._validate_identifier(table)
        return f"{self.database}.{schema}.{table}"

    @staticmethod
    def _fetch_study_counts(cur, table_name: str) -> dict[str, int]:
        cur.execute(
            f"""
            SELECT
                UPPER(TRIM(STUDY_ID)) AS STUDY_ID,
                COUNT(*) AS ROW_COUNT
            FROM {table_name}
            GROUP BY UPPER(TRIM(STUDY_ID))
            ORDER BY STUDY_ID
            """
        )

        return {
            str(row[0]): int(row[1] or 0)
            for row in cur.fetchall()
            if row[0] is not None
        }

    @staticmethod
    def _count_duplicate_groups(
        cur,
        table_name: str,
        business_keys: list[str],
    ) -> int:
        key_sql = ", ".join(business_keys)

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM
            (
                SELECT {key_sql}
                FROM {table_name}
                GROUP BY {key_sql}
                HAVING COUNT(*) > 1
            )
            """
        )

        row = cur.fetchone()
        return int(row[0] or 0)

    @staticmethod
    def _count_null_rows(
        cur,
        table_name: str,
        columns: list[str],
    ) -> int:
        condition = " OR ".join(
            f"{column} IS NULL" for column in columns
        )

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE {condition}
            """
        )

        row = cur.fetchone()
        return int(row[0] or 0)

    @staticmethod
    def _count_rows(cur, table_name: str) -> int:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        row = cur.fetchone()
        return int(row[0] or 0)

    def _reconcile_entity(
        self,
        cur,
        entity_name: str,
        config: dict,
    ) -> EntityReconciliationResult:
        raw_table = self._qualified_table(
            self.raw_schema,
            config["raw_table"],
        )

        target_table = self._qualified_table(
            self.mart_schema,
            config["target_table"],
        )

        business_keys = list(config["business_keys"])
        technical_key = str(config["technical_key"])

        raw_row_count = self._count_rows(cur, raw_table)
        target_row_count = self._count_rows(cur, target_table)

        raw_counts = self._fetch_study_counts(cur, raw_table)
        target_counts = self._fetch_study_counts(cur, target_table)

        all_study_ids = sorted(set(raw_counts) | set(target_counts))

        study_results: list[StudyCountResult] = []

        for study_id in all_study_ids:
            raw_count = raw_counts.get(study_id, 0)
            target_count = target_counts.get(study_id, 0)
            difference = target_count - raw_count

            study_results.append(
                StudyCountResult(
                    study_id=study_id,
                    raw_count=raw_count,
                    target_count=target_count,
                    count_difference=difference,
                    status="SUCCESS" if difference == 0 else "FAILED",
                )
            )

        failed_study_count = sum(
            1 for item in study_results if item.status != "SUCCESS"
        )

        raw_duplicate_groups = self._count_duplicate_groups(
            cur,
            raw_table,
            business_keys,
        )

        target_duplicate_groups = self._count_duplicate_groups(
            cur,
            target_table,
            business_keys,
        )

        raw_null_business_key_rows = self._count_null_rows(
            cur,
            raw_table,
            business_keys,
        )

        target_null_business_key_rows = self._count_null_rows(
            cur,
            target_table,
            business_keys,
        )

        target_null_technical_key_rows = self._count_null_rows(
            cur,
            target_table,
            [technical_key],
        )

        row_count_difference = target_row_count - raw_row_count

        passed = all(
            (
                row_count_difference == 0,
                failed_study_count == 0,
                raw_duplicate_groups == 0,
                target_duplicate_groups == 0,
                raw_null_business_key_rows == 0,
                target_null_business_key_rows == 0,
                target_null_technical_key_rows == 0,
            )
        )

        result = EntityReconciliationResult(
            entity_name=entity_name,
            raw_table=raw_table,
            target_table=target_table,
            raw_row_count=raw_row_count,
            target_row_count=target_row_count,
            row_count_difference=row_count_difference,
            raw_duplicate_groups=raw_duplicate_groups,
            target_duplicate_groups=target_duplicate_groups,
            raw_null_business_key_rows=raw_null_business_key_rows,
            target_null_business_key_rows=target_null_business_key_rows,
            target_null_technical_key_rows=target_null_technical_key_rows,
            failed_study_count=failed_study_count,
            study_results=study_results,
            status="SUCCESS" if passed else "FAILED",
        )

        logger.info(
            (
                "entity_reconciliation_completed "
                "entity=%s status=%s raw_count=%s target_count=%s "
                "difference=%s failed_studies=%s raw_duplicates=%s "
                "target_duplicates=%s raw_null_keys=%s "
                "target_null_keys=%s target_null_technical_keys=%s"
            ),
            result.entity_name,
            result.status,
            result.raw_row_count,
            result.target_row_count,
            result.row_count_difference,
            result.failed_study_count,
            result.raw_duplicate_groups,
            result.target_duplicate_groups,
            result.raw_null_business_key_rows,
            result.target_null_business_key_rows,
            result.target_null_technical_key_rows,
        )

        return result

    def reconcile(self) -> PipelineReconciliationResult:
        logger.info(
            (
                "pipeline_reconciliation_started connection=%s "
                "database=%s raw_schema=%s mart_schema=%s"
            ),
            self.connection_name,
            self.database,
            self.raw_schema,
            self.mart_schema,
        )

        conn = snowflake.connector.connect(
            connection_name=self.connection_name,
            application="ACT_PIPELINE_RECONCILIATION",
        )

        try:
            with conn.cursor() as cur:
                entity_results = [
                    self._reconcile_entity(cur, entity_name, config)
                    for entity_name, config in ENTITY_CONFIG.items()
                ]
        finally:
            conn.close()

        successful_entity_count = sum(
            1 for item in entity_results if item.status == "SUCCESS"
        )

        failed_entity_count = len(entity_results) - successful_entity_count

        result = PipelineReconciliationResult(
            status="SUCCESS" if failed_entity_count == 0 else "FAILED",
            database=self.database,
            raw_schema=self.raw_schema,
            mart_schema=self.mart_schema,
            entity_count=len(entity_results),
            successful_entity_count=successful_entity_count,
            failed_entity_count=failed_entity_count,
            entities=entity_results,
        )

        logger.info(
            (
                "pipeline_reconciliation_completed status=%s "
                "entity_count=%s successful_entities=%s failed_entities=%s"
            ),
            result.status,
            result.entity_count,
            result.successful_entity_count,
            result.failed_entity_count,
        )

        if result.status != "SUCCESS":
            failed_entities = [
                item.entity_name
                for item in entity_results
                if item.status != "SUCCESS"
            ]

            raise RuntimeError(
                "ACT reconciliation failed for entities: "
                + ", ".join(failed_entities)
            )

        return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    print(PipelineReconciler().reconcile().to_dict())
