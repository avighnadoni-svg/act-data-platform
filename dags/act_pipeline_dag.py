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

    schedule=None,

    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),

    catchup=False,

    max_active_runs=1,

    is_paused_upon_creation=False,

    tags=[
        "ACT",
        "Clinical",
        "Rave",
        "S3",
        "Incremental",
        "Multi-Study",
    ],

    description=(
        "Multi-study ACT clinical ingestion "
        "from Rave API to AWS S3"
    ),
)
def act_rave_ingestion():


    # ========================================================
    # TASK 1
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
        Discover studies dynamically from Rave API.

        Example:

            ONC101
            ONC102
            ONC103
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
        # API SESSION
        # ====================================================

        with RaveAPIClient() as client:


            while True:


                # ============================================
                # DEFENSIVE PAGE LIMIT
                # ============================================

                if (
                    pages_processed
                    >= MAX_DISCOVERY_PAGES
                ):

                    raise DataValidationError(
                        (
                            "Maximum study discovery "
                            "page limit exceeded"
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


                # ============================================
                # CALL STUDY API
                # ============================================

                response = (
                    client.get_page(

                        entity_name=
                            "study",

                        offset=
                            offset,

                        limit=
                            DISCOVERY_PAGE_SIZE,
                    )
                )


                # ============================================
                # PARSE JSON
                # ============================================

                page_records = (
                    parse_response(

                        entity_name=
                            "study",

                        raw_text=
                            response.text,
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
                        "record_count=%s"
                    ),
                    pages_processed,
                    page_count,
                )


                # ============================================
                # NO DATA
                # ============================================

                if page_count == 0:

                    break


                # ============================================
                # APPEND
                # ============================================

                all_records.extend(
                    page_records
                )


                # ============================================
                # LAST PAGE
                # ============================================

                if (
                    page_count
                    < DISCOVERY_PAGE_SIZE
                ):

                    break


                # ============================================
                # NEXT PAGE
                # ============================================

                offset += (
                    DISCOVERY_PAGE_SIZE
                )


        # ====================================================
        # NO STUDIES
        # ====================================================

        if not all_records:

            raise DataValidationError(
                "No studies returned from Rave API"
            )


        # ====================================================
        # VALIDATE STUDIES
        # ====================================================

        validation = (
            validate_records(

                entity_name=
                    "study",

                records=
                    all_records,
            )
        )


        # ====================================================
        # STUDY LIST
        # ====================================================

        study_ids = (

            validation
            .dataframe[
                "study_id"
            ]

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


        return study_ids


    # ========================================================
    # TASK 2
    # BUILD STUDY × ENTITY WORK ITEMS
    # ========================================================

    @task(
        task_id="build_ingestion_work_items"
    )
    def build_ingestion_work_items(
        study_ids: list[str],
    ) -> list[dict]:
        """
        Create:

            study × entity

        work items.

        IMPORTANT:

        load_date is generated ONCE here and passed
        downstream.

        We no longer depend on logical_date.
        """

        from config.endpoints import (
            ENDPOINTS,
        )


        if not study_ids:

            raise ValueError(
                "study_ids cannot be empty"
            )


        # ====================================================
        # LOAD DATE
        #
        # Actual UTC ingestion date.
        #
        # Generated once and stored in the XCom work items,
        # therefore all mapped tasks receive the same date.
        # ====================================================

        load_date = (
            pendulum
            .now("UTC")
            .date()
            .isoformat()
        )


        entities = list(
            ENDPOINTS.keys()
        )


        work_items = []


        # ====================================================
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

                        "load_date":
                            load_date,
                    }
                )


        logger.info(
            (
                "ingestion_work_items_created "
                "study_count=%s "
                "entity_count=%s "
                "work_item_count=%s "
                "load_date=%s"
            ),
            len(study_ids),
            len(entities),
            len(work_items),
            load_date,
        )


        return work_items


    # ========================================================
    # TASK 3
    # ONE STUDY + ONE ENTITY
    # ========================================================

    @task(
        task_id="ingest_study_entity",

        map_index_template=(
            "{{ act_map_name }}"
        ),

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

        max_active_tis_per_dag=4,

        max_active_tis_per_dagrun=4,
    )
    def ingest_one_study_entity(
        work_item: dict,
    ) -> dict:
        """
        Execute one ingestion unit.

        Example:

            ONC101 + adverse_event
        """

        from src.api.extract_all import (
            ingest_study_entity,
        )


        # ====================================================
        # AIRFLOW CONTEXT
        # ====================================================

        context = (
            get_current_context()
        )


        # ====================================================
        # WORK ITEM
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


        load_date = (
            work_item[
                "load_date"
            ]
        )


        # ====================================================
        # FRIENDLY MAP NAME
        #
        # Airflow UI:
        #
        # ONC101__adverse_event
        # ====================================================

        context[
            "act_map_name"
        ] = (
            f"{study_id}"
            f"__"
            f"{entity_name}"
        )


        # ====================================================
        # RUN ID
        #
        # run_id is stable across task retries.
        #
        # We deliberately do NOT use logical_date here.
        # ====================================================

        run_id = context[
            "run_id"
        ]


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

        result = (
            ingest_study_entity(

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


        return result_dict


    # ========================================================
    # TASK 4
    # SUMMARY
    # ========================================================

    @task(
        task_id="summarize_ingestion"
    )
    def summarize_ingestion(
        results,
    ) -> dict:
        """
        Summarize all mapped ingestion results.
        """

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

            if (
                result.get(
                    "load_type"
                )
                == "FULL"
            )
        )


        incremental_loads = sum(

            1

            for result in result_list

            if (
                result.get(
                    "load_type"
                )
                == "INCREMENTAL"
            )
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
                "study_count=%s "
                "task_count=%s "
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


    ingestion_results = (

        ingest_one_study_entity

        .expand(
            work_item=
                work_items
        )
    )


    summarize_ingestion(
        ingestion_results
    )


# ============================================================
# CREATE DAG
# ============================================================

dag = act_rave_ingestion()


# ============================================================
# LOCAL TEST
# ============================================================

if __name__ == "__main__":

    dag.test()