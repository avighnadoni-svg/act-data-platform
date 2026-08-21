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


from src.monitoring.alerting import (
    on_task_failure,
    on_task_success,
)


logger = logging.getLogger(
    __name__
)


# ============================================================
# DEFAULT AIRFLOW CALLBACKS
# ============================================================

DEFAULT_ARGS = {
    "on_failure_callback": on_task_failure,
    "on_success_callback": on_task_success,
}


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

    default_args=DEFAULT_ARGS,

    # ========================================================
    # DEVELOPMENT MODE
    #
    # Keep manual while the complete platform is still being
    # developed and validated.
    #
    # Final production cadence can later be changed to:
    #
    #     0 */6 * * *
    #
    # for four runs per day.
    # ========================================================

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
        "Local-Storage",
        "Snowflake-Control",
        "Snowflake-RAW",
        "dbt",
        "Incremental",
        "Multi-Study",
    ],

    description=(
        "Multi-study ACT clinical ingestion from Rave API "
        "through local storage and Snowflake RAW, followed by "
        "dbt dimensional and reporting model builds with "
        "Snowflake watermark and CONTROL auditing"
    ),
)
def act_rave_ingestion():


    # ========================================================
    # TASK 0
    # START PIPELINE RUN AUDIT
    # ========================================================

    @task(
        task_id="start_pipeline_audit",

        # ----------------------------------------------------
        # Do not retry this INSERT-style start operation.
        #
        # This prevents duplicate parent audit rows for the
        # same Airflow DAG run.
        # ----------------------------------------------------

        retries=0,

        execution_timeout=timedelta(
            minutes=5
        ),
    )
    def start_pipeline_audit() -> str:
        """
        Create one PIPELINE_RUN_AUDIT row for this
        Airflow DAG run.

        The row starts as:

            STATUS = RUNNING

        and is completed by finish_pipeline_audit.
        """

        from src.snowflake.control_audit import (
            ControlAuditClient,
        )


        context = (
            get_current_context()
        )


        ti = context[
            "ti"
        ]


        run_id = ti.run_id


        # ====================================================
        # RUN TYPE
        # ====================================================

        if run_id.startswith(
            "manual__"
        ):

            run_type = "MANUAL"


        elif run_id.startswith(
            "scheduled__"
        ):

            run_type = "SCHEDULED"


        else:

            run_type = "OTHER"


        control_audit_client = (
            ControlAuditClient()
        )


        pipeline_audit_id = (
            control_audit_client
            .start_pipeline_run(

                dag_id=
                    ti.dag_id,

                dag_run_id=
                    run_id,

                run_type=
                    run_type,

                triggered_by=
                    "AIRFLOW",
            )
        )


        logger.info(
            (
                "pipeline_audit_started "
                "pipeline_audit_id=%s "
                "dag_id=%s "
                "dag_run_id=%s "
                "run_type=%s"
            ),
            pipeline_audit_id,
            ti.dag_id,
            run_id,
            run_type,
        )


        return pipeline_audit_id


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
    def discover_studies(
        pipeline_audit_id: str,
    ) -> list[str]:
        """
        Discover studies dynamically from Rave API.

        pipeline_audit_id is passed in so study discovery
        cannot begin until the parent pipeline audit exists.

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
            (
                "study_discovery_started "
                "pipeline_audit_id=%s"
            ),
            pipeline_audit_id,
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
                "studies=%s "
                "pipeline_audit_id=%s"
            ),
            len(study_ids),
            study_ids,
            pipeline_audit_id,
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

        We do not depend on logical_date.
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


        ti = context[
            "ti"
        ]


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
        # RUN / TASK METADATA
        # ====================================================
        #
        # Airflow 3 task context exposes this metadata through
        # the public TaskInstance interface.
        # ====================================================

        run_id = ti.run_id

        task_id = ti.task_id

        map_index = ti.map_index

        attempt_number = ti.try_number


        logger.info(
            (
                "mapped_ingestion_started "
                "study_id=%s "
                "entity=%s "
                "run_id=%s "
                "task_id=%s "
                "map_index=%s "
                "attempt=%s "
                "load_date=%s"
            ),
            study_id,
            entity_name,
            run_id,
            task_id,
            map_index,
            attempt_number,
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

                dag_id=
                    ti.dag_id,

                task_id=
                    task_id,

                map_index=
                    map_index,

                attempt_number=
                    attempt_number,
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
        Summarize all successful mapped ingestion results.

        If any mapped ingestion task ultimately fails,
        the normal ALL_SUCCESS trigger rule prevents this
        summary from running.

        finish_pipeline_audit still runs afterward because
        it uses trigger_rule='all_done'.
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
    # TASK 5
    # BUILD SNOWFLAKE RAW ENTITY LIST
    # ========================================================

    @task(
        task_id="build_snowflake_raw_entities"
    )
    def build_snowflake_raw_entities() -> list[str]:
        """
        Return the eight Snowflake RAW entities in the
        established processing order.

        The SQL mapping itself remains centralized in:

            src/snowflake/raw_processor.py
        """

        from src.snowflake.raw_processor import (
            RAW_PROCESS_ORDER,
        )


        entities = list(
            RAW_PROCESS_ORDER
        )


        logger.info(
            (
                "snowflake_raw_entities_created "
                "entity_count=%s "
                "entities=%s"
            ),
            len(entities),
            entities,
        )


        return entities


    # ========================================================
    # TASK 6
    # PROCESS ONE SNOWFLAKE RAW ENTITY
    # ========================================================

    @task(
        task_id="process_snowflake_raw_entity",

        map_index_template=(
            "{{ raw_entity_name }}"
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
    def process_snowflake_raw_entity(
        entity_name: str,
    ) -> dict:
        """
        Execute the existing Snowflake Option 3 SQL
        for exactly one RAW entity.

        Example:

            adverse_event
                ↓
            010_process_adverse_event_option3.sql
                ↓
            LND_ADVERSE_EVENT
                ↓
            RAW_ADVERSE_EVENT_HISTORY
                ↓
            RAW_ADVERSE_EVENT_CURRENT
        """

        from src.snowflake.raw_processor import (
            SnowflakeRawProcessor,
        )


        context = (
            get_current_context()
        )


        context[
            "raw_entity_name"
        ] = entity_name


        logger.info(
            (
                "snowflake_raw_mapped_task_started "
                "entity=%s"
            ),
            entity_name,
        )


        result = (
            SnowflakeRawProcessor()
            .process_entity(
                entity_name
            )
        )


        result_dict = (
            result.to_dict()
        )


        logger.info(
            (
                "snowflake_raw_mapped_task_completed "
                "entity=%s "
                "status=%s "
                "sql_file=%s "
                "statements=%s"
            ),
            result.entity_name,
            result.status,
            result.sql_file,
            result.statements_executed,
        )


        return result_dict


    # ========================================================
    # TASK 7
    # SUMMARIZE SNOWFLAKE RAW PROCESSING
    # ========================================================

    @task(
        task_id="summarize_snowflake_raw"
    )
    def summarize_snowflake_raw(
        results,
    ) -> dict:
        """
        Summarize all successful mapped Snowflake RAW tasks.

        This task runs only when every mapped RAW task succeeds.
        If any RAW task fails, this task becomes upstream_failed
        and finish_pipeline_audit detects that the RAW summary
        is unavailable.
        """

        result_list = list(
            results
        )


        total_tasks = len(
            result_list
        )


        successful_tasks = sum(

            1

            for result in result_list

            if (
                result.get(
                    "status"
                )
                == "SUCCESS"
            )
        )


        entities = sorted(
            [
                result.get(
                    "entity_name"
                )

                for result in result_list

                if result.get(
                    "entity_name"
                )
            ]
        )


        total_statements = sum(

            int(
                result.get(
                    "statements_executed",
                    0,
                )
                or 0
            )

            for result in result_list
        )


        summary = {

            "task_count":
                total_tasks,

            "successful_task_count":
                successful_tasks,

            "entities":
                entities,

            "total_statements_executed":
                total_statements,
        }


        logger.info(
            (
                "snowflake_raw_summary "
                "task_count=%s "
                "successful_task_count=%s "
                "total_statements=%s "
                "entities=%s"
            ),
            summary[
                "task_count"
            ],
            summary[
                "successful_task_count"
            ],
            summary[
                "total_statements_executed"
            ],
            summary[
                "entities"
            ],
        )


        return summary


    # ========================================================
    # TASK 8
    # RUN DBT BUILD
    # ========================================================

    @task(
        task_id="run_dbt_build",

        retries=1,

        retry_delay=timedelta(
            minutes=1
        ),

        execution_timeout=timedelta(
            minutes=45
        ),
    )
    def run_dbt_build(
        raw_summary: dict,
    ) -> dict:
        """
        Build and test the complete ACT dbt project after all
        Snowflake RAW processing has succeeded.

        Airflow runs inside .airflow-venv, while dbt is
        intentionally executed from the project .venv through
        src.dbt.dbt_runner.DbtRunner.

        The task fails when dbt returns a non-zero exit code,
        which prevents a successful pipeline audit.
        """

        from src.dbt.dbt_runner import (
            DbtRunner,
        )


        if not isinstance(
            raw_summary,
            dict,
        ):

            raise RuntimeError(
                "Snowflake RAW summary unavailable before dbt build"
            )


        logger.info(
            (
                "dbt_build_task_started "
                "raw_task_count=%s "
                "raw_successful_task_count=%s"
            ),
            raw_summary.get(
                "task_count"
            ),
            raw_summary.get(
                "successful_task_count"
            ),
        )


        result = (
            DbtRunner()
            .run_build()
        )


        result_dict = (
            result.to_dict()
        )


        logger.info(
            (
                "dbt_build_task_completed "
                "status=%s "
                "return_code=%s "
                "duration_seconds=%s"
            ),
            result.status,
            result.return_code,
            result.duration_seconds,
        )


        return result_dict


    # ========================================================
    # TASK 9
    # RECONCILE RAW VS DBT TARGETS
    # ========================================================

    @task(
        task_id="reconcile_pipeline",

        retries=1,

        retry_delay=timedelta(
            minutes=1
        ),

        execution_timeout=timedelta(
            minutes=15
        ),
    )
    def reconcile_pipeline(
        dbt_summary: dict,
    ) -> dict:
        """
        Reconcile Snowflake RAW current tables against the
        dbt dimensions and facts after dbt build succeeds.

        Validation includes:

            RAW total row count == dbt target total row count
            per-study row counts match
            no duplicate business keys
            no NULL business keys
            no NULL dbt technical keys

        A mismatch raises an exception and therefore prevents
        the pipeline from being reported as successful.
        """

        from src.snowflake.reconciliation import (
            PipelineReconciler,
        )


        if not isinstance(
            dbt_summary,
            dict,
        ):

            raise RuntimeError(
                "dbt summary unavailable before reconciliation"
            )


        if (
            dbt_summary.get(
                "status"
            )
            != "SUCCESS"
        ):

            raise RuntimeError(
                (
                    "dbt build did not succeed before reconciliation. "
                    f"status={dbt_summary.get('status')}"
                )
            )


        logger.info(
            "pipeline_reconciliation_task_started"
        )


        result = (
            PipelineReconciler()
            .reconcile()
        )


        result_dict = (
            result.to_dict()
        )


        logger.info(
            (
                "pipeline_reconciliation_task_completed "
                "status=%s entity_count=%s "
                "successful_entities=%s failed_entities=%s"
            ),
            result.status,
            result.entity_count,
            result.successful_entity_count,
            result.failed_entity_count,
        )


        return result_dict


    # ========================================================
    # TASK 10
    # FINISH PIPELINE RUN AUDIT
    # ========================================================

    @task(
        task_id="finish_pipeline_audit",

        # ----------------------------------------------------
        # Run after upstream work is terminal even when
        # discovery, mapping, ingestion, RAW processing,
        # dbt, or reconciliation failed.
        #
        # This task is bookkeeping. It records SUCCESS/FAILED
        # into PIPELINE_RUN_AUDIT and returns that result.
        #
        # It does NOT intentionally fail just because an
        # upstream task failed. A separate final leaf task
        # enforces the Airflow DAG-run status.
        # ----------------------------------------------------

        trigger_rule="all_done",

        # ----------------------------------------------------
        # One retry is retained only for a genuine transient
        # audit-finalization problem, such as a Snowflake
        # connectivity issue.
        # ----------------------------------------------------

        retries=1,

        retry_delay=timedelta(
            seconds=30
        ),

        execution_timeout=timedelta(
            minutes=10
        ),
    )
    def finish_pipeline_audit(
        pipeline_audit_id: str,
    ) -> dict:
        """
        Finalize one PIPELINE_RUN_AUDIT row.

        Status is determined from:

        1. Expected ingestion work-item count from Airflow XCom.
        2. Latest ENTITY_LOAD_AUDIT status for every
           study + entity in the current DAG run.
        3. Snowflake RAW processing summary.
        4. dbt build result.
        5. RAW-to-dbt reconciliation result.

        SUCCESS requires:

            all study/entity ingestion work items successful

        AND

            all eight Snowflake RAW entity tasks successful

        AND

            dbt build completed successfully

        AND

            RAW-to-dbt reconciliation completed successfully

        Any failed, missing, or incomplete ingestion item,
        Snowflake RAW item, dbt build, or reconciliation check
        makes the pipeline audit FAILED.
        """

        import snowflake.connector

        from config.endpoints import (
            ENDPOINTS,
        )

        from src.snowflake.control_audit import (
            ControlAuditClient,
        )

        from src.snowflake.raw_processor import (
            RAW_PROCESS_ORDER,
        )


        context = (
            get_current_context()
        )


        ti = context[
            "ti"
        ]


        run_id = ti.run_id


        # ====================================================
        # DISCOVERY / WORK-ITEM COUNTS FROM XCOM
        # ====================================================

        study_ids = (
            ti.xcom_pull(
                task_ids=
                    "discover_studies"
            )
        )


        work_items = (
            ti.xcom_pull(
                task_ids=
                    "build_ingestion_work_items"
            )
        )


        raw_summary = (
            ti.xcom_pull(
                task_ids=
                    "summarize_snowflake_raw"
            )
        )


        dbt_summary = (
            ti.xcom_pull(
                task_ids=
                    "run_dbt_build"
            )
        )


        reconciliation_summary = (
            ti.xcom_pull(
                task_ids=
                    "reconcile_pipeline"
            )
        )


        if isinstance(
            study_ids,
            list,
        ):

            studies_discovered = len(
                study_ids
            )


        else:

            studies_discovered = None


        if isinstance(
            work_items,
            list,
        ):

            work_items_created = len(
                work_items
            )


        elif studies_discovered is not None:

            work_items_created = (
                studies_discovered
                * len(
                    ENDPOINTS
                )
            )


        else:

            work_items_created = None


        # ====================================================
        # READ LATEST ENTITY AUDIT RESULT PER BUSINESS UNIT
        # ====================================================
        #
        # A mapped task may be retried. We therefore inspect
        # only the latest audit row for:
        #
        #     STUDY_ID + ENTITY_NAME
        #
        # within this DAG_RUN_ID.
        # ====================================================

        control_audit_client = (
            ControlAuditClient()
        )


        conn = (
            snowflake.connector.connect(
                connection_name=
                    control_audit_client.connection_name,

                application=
                    "ACT_PIPELINE_AUDIT_FINALIZER",
            )
        )


        try:

            with conn.cursor() as cur:

                cur.execute(
                    """
                    WITH LATEST_ENTITY_STATUS AS
                    (
                        SELECT
                            STUDY_ID,
                            ENTITY_NAME,
                            STATUS,

                            ROW_NUMBER() OVER
                            (
                                PARTITION BY
                                    STUDY_ID,
                                    ENTITY_NAME

                                ORDER BY
                                    ENDED_AT DESC NULLS LAST,
                                    UPDATED_AT DESC NULLS LAST,
                                    CREATED_AT DESC NULLS LAST
                            ) AS RN

                        FROM ACT_DB.CONTROL.ENTITY_LOAD_AUDIT

                        WHERE DAG_RUN_ID = %s
                    )

                    SELECT
                        COUNT(*) AS AUDITED_ITEMS,

                        COUNT_IF(
                            STATUS IN (
                                'SUCCESS',
                                'NO_NEW_DATA'
                            )
                        ) AS SUCCESSFUL_ITEMS,

                        COUNT_IF(
                            STATUS = 'FAILED'
                        ) AS FAILED_ITEMS,

                        COUNT_IF(
                            STATUS NOT IN (
                                'SUCCESS',
                                'NO_NEW_DATA',
                                'FAILED'
                            )
                            OR STATUS IS NULL
                        ) AS INCOMPLETE_ITEMS

                    FROM LATEST_ENTITY_STATUS

                    WHERE RN = 1
                    """,
                    (
                        run_id,
                    ),
                )


                row = cur.fetchone()


        finally:

            conn.close()


        audited_items = int(
            row[0] or 0
        )

        successful_items = int(
            row[1] or 0
        )

        failed_items = int(
            row[2] or 0
        )

        incomplete_items = int(
            row[3] or 0
        )


        # ====================================================
        # DETERMINE FINAL PIPELINE STATUS
        # ====================================================

        error_parts = []


        if work_items_created is None:

            error_parts.append(
                "expected work item count unavailable"
            )


        else:

            if (
                audited_items
                != work_items_created
            ):

                error_parts.append(
                    (
                        "audited_items="
                        f"{audited_items} "
                        "expected_items="
                        f"{work_items_created}"
                    )
                )


        if failed_items > 0:

            error_parts.append(
                (
                    "failed_items="
                    f"{failed_items}"
                )
            )


        if incomplete_items > 0:

            error_parts.append(
                (
                    "incomplete_items="
                    f"{incomplete_items}"
                )
            )


        if (
            work_items_created is not None
            and successful_items
            != work_items_created
        ):

            error_parts.append(
                (
                    "successful_items="
                    f"{successful_items} "
                    "expected_items="
                    f"{work_items_created}"
                )
            )


        # ====================================================
        # SNOWFLAKE RAW VALIDATION
        # ====================================================

        expected_raw_tasks = len(
            RAW_PROCESS_ORDER
        )


        raw_task_count = None

        raw_successful_task_count = None


        if not isinstance(
            raw_summary,
            dict,
        ):

            error_parts.append(
                "Snowflake RAW summary unavailable"
            )


        else:

            raw_task_count = (
                raw_summary.get(
                    "task_count"
                )
            )


            raw_successful_task_count = (
                raw_summary.get(
                    "successful_task_count"
                )
            )


            if (
                raw_task_count
                != expected_raw_tasks
            ):

                error_parts.append(
                    (
                        "raw_task_count="
                        f"{raw_task_count} "
                        "expected_raw_tasks="
                        f"{expected_raw_tasks}"
                    )
                )


            if (
                raw_successful_task_count
                != expected_raw_tasks
            ):

                error_parts.append(
                    (
                        "raw_successful_tasks="
                        f"{raw_successful_task_count} "
                        "expected_raw_tasks="
                        f"{expected_raw_tasks}"
                    )
                )


        # ====================================================
        # DBT VALIDATION
        # ====================================================

        dbt_status = None

        dbt_return_code = None


        if not isinstance(
            dbt_summary,
            dict,
        ):

            error_parts.append(
                "dbt build summary unavailable"
            )


        else:

            dbt_status = (
                dbt_summary.get(
                    "status"
                )
            )


            dbt_return_code = (
                dbt_summary.get(
                    "return_code"
                )
            )


            if dbt_status != "SUCCESS":

                error_parts.append(
                    (
                        "dbt_status="
                        f"{dbt_status}"
                    )
                )


            if dbt_return_code != 0:

                error_parts.append(
                    (
                        "dbt_return_code="
                        f"{dbt_return_code}"
                    )
                )


        # ====================================================
        # RECONCILIATION VALIDATION
        # ====================================================

        reconciliation_status = None

        reconciliation_entity_count = None

        reconciliation_failed_entity_count = None


        if not isinstance(
            reconciliation_summary,
            dict,
        ):

            error_parts.append(
                "reconciliation summary unavailable"
            )


        else:

            reconciliation_status = (
                reconciliation_summary.get(
                    "status"
                )
            )


            reconciliation_entity_count = (
                reconciliation_summary.get(
                    "entity_count"
                )
            )


            reconciliation_failed_entity_count = (
                reconciliation_summary.get(
                    "failed_entity_count"
                )
            )


            if reconciliation_status != "SUCCESS":

                error_parts.append(
                    (
                        "reconciliation_status="
                        f"{reconciliation_status}"
                    )
                )


            if reconciliation_failed_entity_count not in (
                0,
                None,
            ):

                error_parts.append(
                    (
                        "reconciliation_failed_entities="
                        f"{reconciliation_failed_entity_count}"
                    )
                )


        if error_parts:

            final_status = "FAILED"

            error_message = "; ".join(
                error_parts
            )


        else:

            final_status = "SUCCESS"

            error_message = None


        # ====================================================
        # COMPLETE PARENT PIPELINE AUDIT
        # ====================================================

        control_audit_client.finish_pipeline_run(

            pipeline_audit_id=
                pipeline_audit_id,

            status=
                final_status,

            studies_discovered=
                studies_discovered,

            work_items_created=
                work_items_created,

            successful_items=
                successful_items,

            failed_items=
                (
                    failed_items
                    + incomplete_items
                    + max(
                        0,
                        (
                            work_items_created
                            or 0
                        )
                        - audited_items,
                    )
                ),

            error_message=
                error_message,
        )


        result = {

            "pipeline_audit_id":
                pipeline_audit_id,

            "dag_run_id":
                run_id,

            "status":
                final_status,

            "studies_discovered":
                studies_discovered,

            "work_items_created":
                work_items_created,

            "audited_items":
                audited_items,

            "successful_items":
                successful_items,

            "failed_items":
                failed_items,

            "incomplete_items":
                incomplete_items,

            "snowflake_raw_task_count":
                raw_task_count,

            "snowflake_raw_successful_task_count":
                raw_successful_task_count,

            "expected_snowflake_raw_tasks":
                expected_raw_tasks,

            "dbt_status":
                dbt_status,

            "dbt_return_code":
                dbt_return_code,

            "reconciliation_status":
                reconciliation_status,

            "reconciliation_entity_count":
                reconciliation_entity_count,

            "reconciliation_failed_entity_count":
                reconciliation_failed_entity_count,

            "error_message":
                error_message,
        }


        logger.info(
            (
                "pipeline_audit_completed "
                "pipeline_audit_id=%s "
                "dag_run_id=%s "
                "status=%s "
                "studies=%s "
                "work_items=%s "
                "audited_items=%s "
                "successful_items=%s "
                "failed_items=%s "
                "incomplete_items=%s "
                "raw_tasks=%s "
                "raw_successful_tasks=%s "
                "expected_raw_tasks=%s "
                "dbt_status=%s "
                "dbt_return_code=%s "
                "reconciliation_status=%s "
                "reconciliation_failed_entities=%s"
            ),
            pipeline_audit_id,
            run_id,
            final_status,
            studies_discovered,
            work_items_created,
            audited_items,
            successful_items,
            failed_items,
            incomplete_items,
            raw_task_count,
            raw_successful_task_count,
            expected_raw_tasks,
            dbt_status,
            dbt_return_code,
            reconciliation_status,
            reconciliation_failed_entity_count,
        )


        # ====================================================
        # RETURN AUDIT RESULT
        # ====================================================
        #
        # This finalizer is bookkeeping only.
        #
        # If the business pipeline failed, this task still
        # succeeds after persisting:
        #
        #     PIPELINE_RUN_AUDIT.STATUS = FAILED
        #
        # The downstream enforce_pipeline_status task is the
        # final Airflow leaf that marks the DAG run FAILED.
        # ====================================================

        return result


    # ========================================================
    # TASK 11
    # ENFORCE FINAL AIRFLOW DAG STATUS
    # ========================================================

    @task(
        task_id="enforce_pipeline_status",

        # ----------------------------------------------------
        # This task intentionally fails when the audit result
        # says the pipeline failed.
        #
        # It must never retry and must not create another
        # Slack/Snowflake alert because the actual failing
        # business task already generated the meaningful alert.
        # ----------------------------------------------------

        retries=0,

        on_failure_callback=None,

        on_success_callback=None,

        execution_timeout=timedelta(
            minutes=2
        ),
    )
    def enforce_pipeline_status(
        audit_result: dict,
    ) -> dict:
        """
        Keep the Airflow DAG-run state consistent with the
        persisted PIPELINE_RUN_AUDIT status.

        The final audit task records the result.

        This leaf task only enforces:

            audit SUCCESS -> task SUCCESS
            audit FAILED  -> task FAILED

        No operational alert is generated here, which avoids
        duplicate alerts such as:

            SOURCE_DISCOVERY_FAILURE
            AUDIT_FAILURE
            AUDIT_FAILURE
            AUDIT_FAILURE
        """

        from airflow.exceptions import (
            AirflowFailException,
        )


        if not isinstance(
            audit_result,
            dict,
        ):

            raise AirflowFailException(
                "Pipeline audit result unavailable"
            )


        final_status = (
            audit_result.get(
                "status"
            )
        )


        if final_status != "SUCCESS":

            error_message = (
                audit_result.get(
                    "error_message"
                )
                or "Pipeline audit reported FAILED"
            )


            logger.error(
                (
                    "pipeline_status_enforcement_failed "
                    "pipeline_audit_id=%s "
                    "dag_run_id=%s "
                    "status=%s "
                    "error=%s"
                ),
                audit_result.get(
                    "pipeline_audit_id"
                ),
                audit_result.get(
                    "dag_run_id"
                ),
                final_status,
                error_message,
            )


            raise AirflowFailException(
                (
                    "ACT pipeline failed. "
                    f"{error_message}"
                )
            )


        logger.info(
            (
                "pipeline_status_enforcement_succeeded "
                "pipeline_audit_id=%s "
                "dag_run_id=%s"
            ),
            audit_result.get(
                "pipeline_audit_id"
            ),
            audit_result.get(
                "dag_run_id"
            ),
        )


        return audit_result


    # ========================================================
    # DAG FLOW
    # ========================================================

    pipeline_audit_id = (
        start_pipeline_audit()
    )


    study_ids = (
        discover_studies(
            pipeline_audit_id
        )
    )


    work_items = (
        build_ingestion_work_items(
            study_ids
        )
    )


    # ========================================================
    # API -> LOCAL STORAGE -> SNOWFLAKE INTERNAL STAGE
    #
    # Current lab:
    #
    #     2 studies × 8 entities
    #     =
    #     16 dynamically mapped tasks
    # ========================================================

    ingestion_results = (

        ingest_one_study_entity

        .expand(
            work_item=
                work_items
        )
    )


    ingestion_summary = (
        summarize_ingestion(
            ingestion_results
        )
    )


    # ========================================================
    # SNOWFLAKE INTERNAL STAGE -> SNOWFLAKE RAW
    #
    # One task per entity, not per study.
    #
    # Each Option 3 SQL file processes all new staged files
    # for that entity across studies.
    # ========================================================

    raw_entities = (
        build_snowflake_raw_entities()
    )


    # --------------------------------------------------------
    # Do not start Snowflake RAW processing until the complete
    # API -> local storage -> stage ingestion has succeeded.
    # --------------------------------------------------------

    ingestion_summary >> raw_entities


    raw_results = (

        process_snowflake_raw_entity

        .expand(
            entity_name=
                raw_entities
        )
    )


    raw_summary = (
        summarize_snowflake_raw(
            raw_results
        )
    )


    # ========================================================
    # DBT BUILD
    # ========================================================

    dbt_result = (
        run_dbt_build(
            raw_summary
        )
    )


    # ========================================================
    # RAW -> DBT RECONCILIATION
    # ========================================================

    reconciliation_result = (
        reconcile_pipeline(
            dbt_result
        )
    )


    # ========================================================
    # PIPELINE FINALIZER
    # ========================================================

    final_audit = (
        finish_pipeline_audit(
            pipeline_audit_id
        )
    )


    # --------------------------------------------------------
    # final_audit waits until reconciliation is terminal.
    #
    # If RAW processing fails:
    #   dbt_result becomes upstream_failed
    #   reconciliation_result becomes upstream_failed
    #
    # If dbt fails:
    #   dbt_result is failed
    #   reconciliation_result becomes upstream_failed
    #
    # If reconciliation fails:
    #   reconciliation_result is failed
    #
    # In all cases final_audit still runs because it uses
    # trigger_rule=all_done and persists the final audit state.
    # --------------------------------------------------------

    reconciliation_result >> final_audit


    # ========================================================
    # FINAL DAG-STATUS ENFORCEMENT
    # ========================================================
    #
    # Keeping this separate from finish_pipeline_audit prevents
    # the audit task from retrying and generating repeated
    # AUDIT_FAILURE Slack alerts when the real failure occurred
    # earlier in the pipeline.
    #
    # This is the only final leaf task:
    #
    #     audit SUCCESS -> DAG SUCCESS
    #     audit FAILED  -> DAG FAILED
    #
    # It has no failure callback, so the original task remains
    # the source of the operational Slack alert.
    # ========================================================

    enforce_pipeline_status(
        final_audit
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
