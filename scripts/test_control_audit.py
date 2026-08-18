#!/usr/bin/env python3
"""
Smoke test for ACT Snowflake CONTROL auditing.

The test:
1. Inserts a temporary PIPELINE_RUN_AUDIT row.
2. Marks it SUCCESS.
3. Reads it back and validates the status.
4. Deletes the temporary row.

No permanent test audit row is left behind.
"""

from __future__ import annotations

from pathlib import Path
import sys
from uuid import uuid4

import snowflake.connector


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.snowflake.control_audit import (
    ControlAuditClient,
)


def main() -> None:
    dag_run_id = (
        "CONTROL_SMOKE_TEST__"
        + str(uuid4())
    )

    client = ControlAuditClient()

    audit_id = client.start_pipeline_run(
        dag_id="act_rave_ingestion",
        dag_run_id=dag_run_id,
        run_type="TEST",
        triggered_by="scripts/test_control_audit.py",
        studies_discovered=2,
        work_items_created=16,
    )

    client.finish_pipeline_run(
        pipeline_audit_id=audit_id,
        status="SUCCESS",
        studies_discovered=2,
        work_items_created=16,
        successful_items=16,
        failed_items=0,
    )

    conn = snowflake.connector.connect(
        connection_name=client.connection_name,
        application="ACT_DATA_PLATFORM_CONTROL_TEST",
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    DAG_ID,
                    DAG_RUN_ID,
                    STATUS,
                    STUDIES_DISCOVERED,
                    WORK_ITEMS_CREATED,
                    SUCCESSFUL_ITEMS,
                    FAILED_ITEMS,
                    STARTED_AT,
                    ENDED_AT
                FROM ACT_DB.CONTROL.PIPELINE_RUN_AUDIT
                WHERE PIPELINE_AUDIT_ID = %s
                """,
                (audit_id,),
            )

            row = cur.fetchone()

            if row is None:
                raise RuntimeError(
                    "CONTROL smoke-test row was not found"
                )

            if row[2] != "SUCCESS":
                raise RuntimeError(
                    f"Expected SUCCESS but found {row[2]}"
                )

            print()
            print("CONTROL AUDIT SMOKE TEST")
            print("========================")
            print(f"pipeline_audit_id: {audit_id}")
            print(f"dag_id: {row[0]}")
            print(f"dag_run_id: {row[1]}")
            print(f"status: {row[2]}")
            print(f"studies_discovered: {row[3]}")
            print(f"work_items_created: {row[4]}")
            print(f"successful_items: {row[5]}")
            print(f"failed_items: {row[6]}")
            print(f"started_at: {row[7]}")
            print(f"ended_at: {row[8]}")
            print("result: PASS")

            cur.execute(
                """
                DELETE FROM ACT_DB.CONTROL.PIPELINE_RUN_AUDIT
                WHERE PIPELINE_AUDIT_ID = %s
                """,
                (audit_id,),
            )

            conn.commit()

            print("cleanup: PASS")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
