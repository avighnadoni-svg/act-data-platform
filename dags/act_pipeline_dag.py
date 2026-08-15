# dags/act_pipeline_dag.py

import sys
from pathlib import Path
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


# ============================================================
# PROJECT PATH SETUP
# ============================================================
# Project structure:
#
# act-data-platform/
# ├── dags/
# │   └── act_pipeline_dag.py
# ├── src/
# │   ├── api/
# │   │   ├── rave_client.py
# │   │   └── extract_all.py
# │   └── aws/
# │       └── s3_client.py
# └── config/
#     └── endpoints.py
#
# Add project root so Airflow can import src and config.
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# DAG DEFINITION
# ============================================================

@dag(
    dag_id="act_rave_ingestion",

    # Manual execution for now
    schedule=None,

    # Required start date
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC"
    ),

    # Do not execute historical runs
    catchup=False,

    # Helpful in Airflow UI
    tags=[
        "ACT",
        "Clinical",
        "Rave",
        "S3"
    ],

    description="Extract ACT clinical data from Rave API and load CSV files to AWS S3",
)
def act_rave_ingestion():

    # ========================================================
    # TASK 1 — EXTRACT API DATA AND LOAD TO S3
    # ========================================================

    @task(
        task_id="extract_rave_data_to_s3",

        # Retry task twice if something fails
        retries=2,

        # Wait 30 seconds between retries
        retry_delay=timedelta(seconds=30),

        # Optional exponential retry
        retry_exponential_backoff=True,

        # Do not wait more than 5 minutes between retries
        max_retry_delay=timedelta(minutes=5),

        # Fail if task runs too long
        execution_timeout=timedelta(minutes=15),
    )
    def extract_rave_data_to_s3():

        from src.api.extract_all import extract_all

        print("=" * 60)
        print("ACT RAVE INGESTION STARTED")
        print("=" * 60)

        # ----------------------------------------------------
        # extract_all() performs:
        #
        # Rave API
        #    ↓
        # JSON
        #    ↓
        # Pandas DataFrame
        #    ↓
        # CSV in memory
        #    ↓
        # AWS S3
        # ----------------------------------------------------

        s3_locations = extract_all()

        # ----------------------------------------------------
        # VALIDATION
        # ----------------------------------------------------

        if not s3_locations:
            raise ValueError(
                "No files were uploaded to S3"
            )

        expected_file_count = 8

        if len(s3_locations) != expected_file_count:

            raise ValueError(
                f"Expected {expected_file_count} datasets "
                f"but only {len(s3_locations)} were uploaded"
            )

        # ----------------------------------------------------
        # LOG SUCCESSFUL FILES
        # ----------------------------------------------------

        print("\n" + "=" * 60)
        print("S3 UPLOAD SUMMARY")
        print("=" * 60)

        for location in s3_locations:
            print(f"SUCCESS: {location}")

        print("\nTotal datasets uploaded:", len(s3_locations))

        print("=" * 60)
        print("ACT RAVE INGESTION COMPLETED SUCCESSFULLY")
        print("=" * 60)

        # Airflow will automatically push this return value
        # through XCom if required by downstream tasks.
        return s3_locations


    # ========================================================
    # TASK EXECUTION
    # ========================================================

    extract_rave_data_to_s3()


# ============================================================
# CREATE DAG OBJECT
# ============================================================

dag = act_rave_ingestion()


# ============================================================
# LOCAL DAG TEST
# ============================================================

if __name__ == "__main__":
    dag.test()