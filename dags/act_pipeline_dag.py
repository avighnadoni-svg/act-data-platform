# dags/act_pipeline_dag.py

import logging
import sys

from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import (
    dag,
    get_current_context,
    task,
)


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

if str(PROJECT_ROOT) not in sys.path:

    sys.path.insert(
        0,
        str(PROJECT_ROOT),
    )


logger = logging.getLogger(
    __name__
)


# ============================================================
# DAG SETTINGS
# ============================================================

DAG_ID = "act_rave_ingestion"

DISCOVERY_PAGE_SIZE = 100

MAX_DISCOVERY_PAGES = 1000


# ============================================================
# DAG
# ============================================================

@dag(

    dag_id=DAG_ID,

    # --------------------------------------------------------
    # Manual execution for now.
    #
    # Later we can change this to:
    #
    # schedule="0 */6 * * *"
    #
    # or another production schedule.
    # --------------------------------------------------------

    schedule=None,

    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),

    catchup=False,

    # --------------------------------------------------------
    # IMPORTANT
    #
    # Do not allow two ACT ingestion DagRuns to execute
    # simultaneously.
    #
    # This protects study/entity watermark state.
    # --------------------------------------------------------

    max_active_runs=1,

    tags=[
        "ACT",
        "Clinical",
        "Rave",
        "S3",
        "Incremental",
        "Multi-Study",
    ],

    description=(
        "Multi-study ACT clinical ingestion from "
        "Rave API to AWS S3 with incremental watermarks"
    ),
)
def act_rave_ingestion():


    # ========================================================
    # TASK 1
    #
    # DISCOVER STUDIES
    # ========================================================

    @task(
        task_id="discover_studies",

        retries=2,

        retry_delay=timedelta(
            seconds=30
        ),

        retry_exponential_backoff=True,

        max_retry_delay=timedelta(
            minutes=5
        ),

        execution_timeout=timedelta(
            minutes=10
        ),
    )
    def discover_studies() -> list[str]:
        """
        Discover all studies currently available
        from the Rave source.

        IMPORTANT:

        API call happens at TASK RUNTIME,
        not while Airflow parses the DAG.

        Example output:

            [
                "ONC101",
                "ONC102",
                "ONC103"
            ]
        """

        from src.api.rave_client import (
            RaveAPIClient,
        )

        from src.common.exceptions import (
            DataValidationError,
        )

        from src.parsers.parser_factory import (
            parse_response,
        )

        from src.processing.validator import (
            validate_records,
        )


        logger.info(
            "study_discovery_started"
        )


        all_records = []

        offset = 0

        pages_processed = 0


        # ====================================================
        # OPEN API SESSION
        # ====================================================

        with RaveAPIClient() as client:


            # =================================================
            # PAGINATION
            # =================================================

            while True:


                # ---------------------------------------------
                # DEFENSIVE LIMIT
                # ---------------------------------------------

                if (
                    pages_processed
                    >= MAX_DISCOVERY_PAGES
                ):

                    raise DataValidationError(
                        (
                            "Maximum study discovery "
                            "page limit exceeded "
                            f"limit="
                            f"{MAX_DISCOVERY_PAGES}"
                        )
                    )


                logger.info(
                    (
                        "study_discovery_page_started "
                        "offset=%s "
                        "limit=%s"
                    ),
                    offset,
                    DISCOVERY_PAGE_SIZE,
                )


                # =============================================
                # STUDIES API
                # =============================================

                response = client.get_page(

                    entity_name="study",

                    offset=offset,

                    limit=DISCOVERY_PAGE_SIZE,
                )


                # =============================================
                # JSON -> FLAT RECORDS
                # =============================================

                page_records = (
                    parse_response(

                        entity_name="study",

                        raw_text=response.text,
                    )
                )


                page_count = len(
                    page_records
                )


                pages_processed += 1


                logger.info(
                    (
                        "study_discovery_page_completed "
                        "page_number=%s "
                        "records=%s"
                    ),
                    pages_processed,
                    page_count,
                )


                # =============================================
                # EMPTY PAGE
                # =============================================

                if page_count == 0:

                    break


                all_records.extend(
                    page_records
                )


                # =============================================
                # LAST PAGE
                # =============================================

                if (
                    page_count
                    < DISCOVERY_PAGE_SIZE
                ):

                    break


                # =============================================
                # NEXT PAGE
                # =============================================

                offset += (
                    DISCOVERY_PAGE_SIZE
                )


        # ====================================================
        # NO STUDIES
        # ====================================================

        if not all_records:

            raise DataValidationError(
                (
                    "No studies returned "
                    "from Rave source"
                )
            )


        # ====================================================
        # VALIDATE STUDY DATA
        # ====================================================

        validation = (
            validate_records(

                entity_name="study",

                records=all_records,
            )
        )


        # ====================================================
        # STUDY IDS
        # ====================================================

        study_ids = (

            validation
            .dataframe["study_id"]

            .astype(str)

            .str.strip()

            .str.upper()

            .drop_duplicates()

            .sort_values()

            .tolist()
        )


        if not study_ids:

            raise DataValidationError(
                (
                    "No valid study IDs "
                    "found during discovery"
                )
            )


        logger.info(
            (
                "study_discovery_completed "
                "study_count=%s "
                "studies=%s"
            ),
            len(study_ids),
            study_ids,
        )


        # ----------------------------------------------------
        # Small metadata only.
        #
        # Perfectly suitable for XCom.
        # ----------------------------------------------------

        return study_ids


    # ========================================================
    # TASK 2
    #
    # BUILD STUDY × ENTITY MATRIX
    # ========================================================

    @task(
        task_id="build_ingestion_work_items"
    )
    def build_ingestion_work_items(
        study_ids: list[str],
    ) -> list[dict]:
        """
        Build runtime work items.

        Example:

        One study:

            ONC101

        produces eight work items:

            ONC101 + study
            ONC101 + site
            ONC101 + subject
            ONC101 + visit
            ONC101 + adverse_event
            ONC101 + lab_result
            ONC101 + protocol_deviation
            ONC101 + data_query


        Two studies:

            2 × 8
            =
            16 mapped tasks
        """

        from config.endpoints import (
            ENDPOINTS,
        )


        if not study_ids:

            raise ValueError(
                "study_ids cannot be empty"
            )


        entities = list(
            ENDPOINTS.keys()
        )


        work_items = []


        # ====================================================
        # CROSS PRODUCT
        #
        # STUDY × ENTITY
        # ====================================================

        for study_id in study_ids:

            for entity_name in entities:

                work_items.append(
                    {
                        "study_id":
                            study_id,

                        "entity_name":
                            entity_name,
                    }
                )


        logger.info(
            (
                "ingestion_work_items_created "
                "study_count=%s "
                "entity_count=%s "
                "work_item_count=%s"
            ),
            len(study_ids),
            len(entities),
            len(work_items),
        )


        return work_items


    # ========================================================
    # TASK 3
    #
    # INGEST ONE STUDY + ONE ENTITY
    #
    # DYNAMICALLY MAPPED
    # ========================================================

    @task(

        task_id="ingest_study_entity",

        # ----------------------------------------------------
        # Friendly names in Airflow UI.
        #
        # Instead of:
        #
        # ingest_study_entity[0]
        #
        # we will see:
        #
        # ONC101__adverse_event
        # ----------------------------------------------------

        map_index_template=(
            "{{ act_map_name }}"
        ),


        # ----------------------------------------------------
        # TASK RETRIES
        #
        # HTTP client itself handles short transient
        # request-level retries.
        #
        # Airflow retry handles the complete transaction:
        #
        # API
        # parse
        # validate
        # S3
        # watermark
        # ----------------------------------------------------

        retries=2,

        retry_delay=timedelta(
            seconds=30
        ),

        retry_exponential_backoff=True,

        max_retry_delay=timedelta(
            minutes=5
        ),

        execution_timeout=timedelta(
            minutes=30
        ),


        # ----------------------------------------------------
        # CONCURRENCY
        #
        # Do not execute all study/entity calls at once.
        #
        # Maximum 4 mapped instances concurrently.
        # ----------------------------------------------------

        max_active_tis_per_dag=4,

        max_active_tis_per_dagrun=4,
    )
    def ingest_one_study_entity(
        work_item: dict,
    ) -> dict:
        """
        Execute one complete incremental ingestion unit.

        Example:

            ONC101
               +
            adverse_event
        """

        from src.api.extract_all import (
            ingest_study_entity,
        )


        # ====================================================
        # TASK CONTEXT
        # ====================================================

        context = (
            get_current_context()
        )


        # ====================================================
        # STUDY + ENTITY
        # ====================================================

        study_id = (
            work_item[
                "study_id"
            ]
        )

        entity_name = (
            work_item[
                "entity_name"
            ]
        )


        # ====================================================
        # FRIENDLY MAPPED TASK NAME
        # ====================================================

        context[
            "act_map_name"
        ] = (
            f"{study_id}"
            f"__"
            f"{entity_name}"
        )


        # ====================================================
        # AIRFLOW RUN ID
        #
        # Stable across retries.
        #
        # Example:
        #
        # manual__2026-08-16T11:50:00+00:00
        # ====================================================

        run_id = context[
            "run_id"
        ]


        # ====================================================
        # LOAD DATE
        #
        # Use the DagRun logical date so the value stays
        # stable if this task retries.
        # ====================================================

        logical_date = context[
            "logical_date"
        ]


        load_date = (
            logical_date
            .in_timezone(
                "UTC"
            )
            .date()
            .isoformat()
        )


        logger.info(
            (
                "mapped_ingestion_started "
                "study_id=%s "
                "entity=%s "
                "run_id=%s "
                "load_date=%s"
            ),
            study_id,
            entity_name,
            run_id,
            load_date,
        )


        # ====================================================
        # COMPLETE INGESTION
        # ====================================================
        #
        # This performs:
        #
        # watermark read
        #      ↓
        # Rave API
        #      ↓
        # pagination
        #      ↓
        # JSON/XML/CSV parsing
        #      ↓
        # validation
        #      ↓
        # Pandas normalization
        #      ↓
        # S3 upload
        #      ↓
        # S3 verification
        #      ↓
        # watermark commit
        #
        # IMPORTANT:
        #
        # commit_watermark=True
        #
        # We are now inside a real Airflow task context.
        # ====================================================

        result = ingest_study_entity(

            study_id=
                study_id,

            entity_name=
                entity_name,

            run_id=
                run_id,

            load_date=
                load_date,

            commit_watermark=
                True,
        )


        result_dict = (
            result.to_dict()
        )


        logger.info(
            (
                "mapped_ingestion_completed "
                "study_id=%s "
                "entity=%s "
                "load_type=%s "
                "records=%s "
                "uploaded=%s "
                "watermark_committed=%s"
            ),
            result.study_id,
            result.entity_name,
            result.load_type,
            result.records_received,
            result.uploaded,
            result.watermark_committed,
        )


        # ----------------------------------------------------
        # Only small metadata is returned through XCom.
        #
        # Data itself remains in S3.
        # ----------------------------------------------------

        return result_dict


    # ========================================================
    # TASK 4
    #
    # SUMMARY / REDUCE
    # ========================================================

    @task(
        task_id="summarize_ingestion"
    )
    def summarize_ingestion(
        results,
    ) -> dict:
        """
        Summarize all mapped ingestion results.

        This task runs only after all mapped tasks
        finish successfully.
        """

        # ----------------------------------------------------
        # Mapped output can arrive as a lazy sequence.
        # Convert once to normal list.
        # ----------------------------------------------------

        result_list = list(
            results
        )


        total_tasks = len(
            result_list
        )


        uploaded_tasks = sum(

            1

            for result in result_list

            if result.get(
                "uploaded"
            )
        )


        no_data_tasks = sum(

            1

            for result in result_list

            if (
                result.get(
                    "records_received",
                    0,
                )
                == 0
            )
        )


        total_records = sum(

            result.get(
                "records_received",
                0,
            )

            for result in result_list
        )


        full_loads = sum(

            1

            for result in result_list

            if result.get(
                "load_type"
            )
            == "FULL"
        )


        incremental_loads = sum(

            1

            for result in result_list

            if result.get(
                "load_type"
            )
            == "INCREMENTAL"
        )


        studies = sorted(
            {
                result.get(
                    "study_id"
                )

                for result in result_list

                if result.get(
                    "study_id"
                )
            }
        )


        summary = {

            "study_count":
                len(studies),

            "studies":
                studies,

            "task_count":
                total_tasks,

            "uploaded_task_count":
                uploaded_tasks,

            "no_data_task_count":
                no_data_tasks,

            "total_records":
                total_records,

            "full_load_count":
                full_loads,

            "incremental_load_count":
                incremental_loads,
        }


        logger.info(
            (
                "act_ingestion_summary "
                "studies=%s "
                "tasks=%s "
                "uploaded=%s "
                "no_data=%s "
                "records=%s "
                "full_loads=%s "
                "incremental_loads=%s"
            ),
            summary[
                "study_count"
            ],
            summary[
                "task_count"
            ],
            summary[
                "uploaded_task_count"
            ],
            summary[
                "no_data_task_count"
            ],
            summary[
                "total_records"
            ],
            summary[
                "full_load_count"
            ],
            summary[
                "incremental_load_count"
            ],
        )


        return summary


    # ========================================================
    # DAG FLOW
    # ========================================================

    study_ids = (
        discover_studies()
    )


    work_items = (
        build_ingestion_work_items(
            study_ids
        )
    )


    # ========================================================
    # DYNAMIC TASK MAPPING
    # ========================================================
    #
    # Airflow determines at runtime how many mapped
    # instances are required.
    #
    # Example:
    #
    # 1 study × 8 entities
    #
    #     8 mapped task instances
    #
    # 10 studies × 8 entities
    #
    #     80 mapped task instances
    # ========================================================

    ingestion_results = (
        ingest_one_study_entity
        .expand(
            work_item=work_items
        )
    )


    summarize_ingestion(
        ingestion_results
    )


# ============================================================
# DAG OBJECT
# ============================================================

dag = act_rave_ingestion()


# ============================================================
# LOCAL DAG TEST
# ============================================================

if __name__ == "__main__":

    dag.test()