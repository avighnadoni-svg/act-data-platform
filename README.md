# ACT Data Platform

Hands-on ingestion platform for **ACT (Automated Case Transfer)** clinical data.

The current implemented phase extracts multi-format clinical data from the mock Rave FastAPI source, validates and normalizes it, and writes study-partitioned CSV files directly to Amazon S3 using Airflow orchestration and per-study/per-entity incremental watermarks.

---

## 1. Current architecture

```text
Mock Rave PostgreSQL
        ↓
FastAPI
        ↓
JSON / XML / CSV
        ↓
Airflow DAG
        ↓
Discover Studies
        ↓
Study × Entity Dynamic Mapping
        ↓
Rave API Client
        ↓
Parser Factory
        ↓
Validation
        ↓
Normalization
        ↓
DataFrame → CSV in memory
        ↓
Amazon S3 RAW
        ↓
Watermark Commit
```

Current implemented scope ends at **Amazon S3 RAW**.

Snowflake/dbt processing is the next phase after source-to-S3 incremental behavior is fully proven.

---

## 2. Current entities

```text
study
site
subject
visit
adverse_event
lab_result
protocol_deviation
data_query
```

For two studies, one DAG run can dynamically create:

```text
2 studies × 8 entities = 16 mapped ingestion tasks
```

---

## 3. Repository structure

```text
act-data-platform/
├── dags/
│   └── act_pipeline_dag.py
├── config/
│   └── endpoints.py
├── src/
│   ├── api/
│   │   ├── rave_client.py
│   │   └── extract_all.py
│   ├── parsers/
│   │   ├── json_parser.py
│   │   ├── xml_parser.py
│   │   ├── csv_parser.py
│   │   └── parser_factory.py
│   ├── processing/
│   │   ├── validator.py
│   │   ├── normalizer.py
│   │   └── json_to_csv.py
│   ├── aws/
│   │   └── s3_client.py
│   ├── watermark/
│   │   ├── __init__.py
│   │   └── watermark_manager.py
│   ├── common/
│   │   ├── exceptions.py
│   │   └── logging_config.py
│   └── snowflake/
│       └── snowflake_client.py
├── tests/
├── requirements-app.txt
├── .env
└── README.md
```

`src/processing/json_to_csv.py` is legacy code and should not be the preferred path for the current generic multi-format framework.

---

# S3 DESIGN

## 4. RAW S3 layout

The chosen partition order is:

```text
study
  ↓
entity
  ↓
load_date
  ↓
run_id
```

Example:

```text
s3://act-clinical-data-dev/
└── act/
    └── raw/
        ├── study_id=ONC101/
        │   ├── adverse_event/
        │   │   └── load_date=2026-08-16/
        │   │       └── run_id=<airflow-run-id>/
        │   │           └── adverse_event.csv
        │   ├── data_query/
        │   ├── lab_result/
        │   ├── protocol_deviation/
        │   ├── site/
        │   ├── study/
        │   ├── subject/
        │   └── visit/
        │
        └── study_id=ONC102/
            ├── adverse_event/
            ├── data_query/
            ├── lab_result/
            ├── protocol_deviation/
            ├── site/
            ├── study/
            ├── subject/
            └── visit/
```

A new Airflow DAG run receives a new Airflow `run_id`, including when the DAG is triggered manually.

A retry of the **same Airflow run** uses the same deterministic S3 key. A completely new DAG run intentionally writes under a different `run_id` path.

---

# ENVIRONMENT

## 5. Required environment variables

The `.env` file is local only and must not be committed.

Expected variables:

```bash
AWS_ACCESS_KEY_ID=<local-lab-value>
AWS_SECRET_ACCESS_KEY=<local-lab-value>
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=act-clinical-data-dev
RAVE_API_BASE_URL=http://127.0.0.1:8000
RAVE_CODESPACE_TOKEN=<only-if-required-for-private-remote-url>
```

For the local Codespaces-to-Codespaces request path, prefer the local Rave endpoint when both repositories run in the same Codespace:

```text
http://127.0.0.1:8000
```

Do not print AWS secret keys or tokens in logs.

Production design should use IAM roles/temporary credentials and managed secrets rather than long-lived keys in `.env`.

---

# START / STOP RUNBOOK

## 6. Correct startup order

```text
1. Start PostgreSQL / Rave source
        ↓
2. Start FastAPI on port 8000
        ↓
3. Verify /health and /studies
        ↓
4. Open act-data-platform terminal
        ↓
5. Activate Python environment
        ↓
6. Load .env
        ↓
7. Set AIRFLOW_HOME
        ↓
8. Start airflow standalone
        ↓
9. Open Airflow UI on port 8080
        ↓
10. Trigger or schedule act_rave_ingestion
```

---

## 7. Terminal 1 — start Airflow

```bash
cd /workspaces/act-data-platform
```

Activate the project environment:

```bash
source .venv/bin/activate
```

Load environment variables:

```bash
set -a
source .env
set +a
```

Set Airflow home explicitly:

```bash
export AIRFLOW_HOME=/workspaces/act-data-platform/airflow
```

Confirm:

```bash
echo $AIRFLOW_HOME
```

Start Airflow:

```bash
airflow standalone
```

Keep this terminal running.

`airflow standalone` is the local all-in-one Airflow mode used for this hands-on environment.

---

## 8. Terminal 2 — Airflow CLI terminal

Open a second terminal:

```bash
cd /workspaces/act-data-platform
source .venv/bin/activate
set -a
source .env
set +a
export AIRFLOW_HOME=/workspaces/act-data-platform/airflow
```

Use Terminal 2 for DAG, task, Variable, and debugging commands.

---

## 9. Stop Airflow

In the terminal running:

```bash
airflow standalone
```

use:

```text
CTRL + C
```

Wait for the Airflow processes to stop cleanly before restarting.

---

# AIRFLOW UI

## 10. Open the UI

Airflow UI/API server uses port:

```text
8080
```

First verify locally:

```bash
curl -I http://127.0.0.1:8080
```

In GitHub Codespaces:

```text
PORTS
  ↓
8080
  ↓
Open in Browser
```

The forwarded URL normally looks like:

```text
https://<codespace-name>-8080.app.github.dev
```

---

## 11. Airflow login password

Airflow 3.x standalone with Simple Auth Manager generates the password file under `AIRFLOW_HOME`.

Read it with:

```bash
cat $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated
```

Typical structure:

```json
{
  "admin": "generated-password"
}
```

Login:

```text
Username: admin
Password: value stored for admin in the generated JSON file
```

If configuration contains:

```text
admin:admin
```

that means:

```text
username = admin
role     = admin
```

It does not mean the password is `admin`.

---

## 12. Change the local lab Airflow password

Stop `airflow standalone` first.

Open:

```bash
nano $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated
```

Change only the password value, for example:

```json
{
  "admin": "your-local-lab-password"
}
```

Save the file, restart:

```bash
airflow standalone
```

This Simple Auth Manager setup is for development/testing, not production authentication architecture.

---

# DAG COMMANDS

## 13. Check Airflow version

```bash
airflow version
```

Expected lab major/minor version:

```text
3.3.x
```

---

## 14. List DAGs

```bash
airflow dags list
```

Look for:

```text
act_rave_ingestion
```

---

## 15. Check DAG import errors

Run this whenever the DAG does not appear in the UI:

```bash
airflow dags list-import-errors
```

Healthy result:

```text
No data found
```

For machine-readable output:

```bash
airflow dags list-import-errors --output=json
```

Healthy JSON result:

```json
[]
```

---

## 16. Show DAG details

```bash
airflow dags details act_rave_ingestion
```

---

## 17. Pause / unpause DAG

Pause:

```bash
airflow dags pause -y act_rave_ingestion
```

Unpause:

```bash
airflow dags unpause -y act_rave_ingestion
```

A manually triggered DAG that is paused can remain queued without scheduling its tasks.

---

## 18. Trigger the DAG manually

```bash
airflow dags trigger act_rave_ingestion
```

Every manual trigger gets a new Airflow `run_id`.

---

## 19. List DAG runs

```bash
airflow dags list-runs act_rave_ingestion
```

Only failed runs:

```bash
airflow dags list-runs act_rave_ingestion --state failed
```

Only running runs:

```bash
airflow dags list-runs act_rave_ingestion --state running
```

---

## 20. List DAG tasks

```bash
airflow tasks list act_rave_ingestion
```

Expected logical tasks:

```text
discover_studies
build_ingestion_work_items
ingest_study_entity
summarize_ingestion
```

`ingest_study_entity` is dynamically mapped at runtime.

---

## 21. Check all task states for one DAG run

First get the run ID:

```bash
airflow dags list-runs act_rave_ingestion
```

Then:

```bash
airflow tasks states-for-dag-run \
  act_rave_ingestion \
  "<RUN_ID>"
```

Example pattern:

```text
manual__2026-08-16T...
```

---

# SCHEDULING

## 22. Batch requirement — 4 runs per day

Target cadence:

```text
4 runs/day = every 6 hours
```

UTC example:

```text
00:00 UTC
06:00 UTC
12:00 UTC
18:00 UTC
```

Cron expression:

```python
schedule="0 */6 * * *"
```

Equivalent IST times when the DAG schedule itself is UTC:

```text
05:30 IST
11:30 IST
17:30 IST
23:30 IST
```

If the business requirement is specifically midnight/6AM/noon/6PM **India time**, configure the DAG/timetable timezone as `Asia/Kolkata` instead of treating the cron as UTC.

During hands-on development, the DAG may deliberately remain `schedule=None` and be triggered manually. Update the README statement when the actual DAG code is switched to scheduled mode.

Keep:

```python
catchup=False
max_active_runs=1
```

for the current lab design unless the scheduling requirements are intentionally changed.

After enabling a schedule, inspect the next execution with:

```bash
airflow dags next-execution act_rave_ingestion
```

---

# WATERMARKS

## 23. Watermark design

Watermarks are maintained independently by:

```text
study_id + entity
```

Examples:

```text
act_watermark__ONC101__adverse_event
act_watermark__ONC102__adverse_event
```

This allows:

```text
ONC101 → incremental position A
ONC102 → incremental position B
```

without one study affecting the other.

Current lab storage is Airflow Variables. A production implementation can move control state into a dedicated metadata/control table.

---

## 24. List watermark Variables

```bash
airflow variables list | grep act_watermark
```

ONC101 only:

```bash
airflow variables list | grep ONC101
```

ONC102 only:

```bash
airflow variables list | grep ONC102
```

For two studies and eight entities, expect up to:

```text
16 study/entity watermark keys
```

once every entity has successfully processed data and committed its watermark.

---

## 25. Read one watermark

```bash
airflow variables get act_watermark__ONC101__adverse_event
```

```bash
airflow variables get act_watermark__ONC102__adverse_event
```

---

## 26. Important Variable debugging rule

Outside an Airflow task, use the **Airflow CLI** to inspect/set Variables.

Do not use a standalone plain-Python call to the Airflow Task SDK Variable API as the primary debugging method. The current pipeline uses the SDK inside actual task execution context.

---

# INCREMENTAL LOGIC

## 27. Full vs incremental behavior

First successful load for a study/entity:

```text
No stored watermark
      ↓
FULL extraction
      ↓
S3 upload verified
      ↓
Watermark committed
```

Later run:

```text
Stored watermark
      ↓
Subtract small overlap
      ↓
updated_since sent to API
      ↓
INCREMENTAL extraction
      ↓
S3 upload verified
      ↓
Watermark advanced
```

Current overlap:

```text
5 seconds
```

The overlap protects against strict source timestamp boundaries but can intentionally re-read some boundary records.

Therefore:

```text
RAW S3 may contain overlap/replay rows across different DAG runs.
```

This is expected. Downstream Snowflake processing will implement deduplication using business key plus change timestamp/version logic.

---

## 28. Watermark safety rule

Watermark update occurs only after:

```text
API extraction
    ↓
parsing
    ↓
validation
    ↓
normalization
    ↓
S3 PUT
    ↓
S3 HEAD/checksum verification
    ↓
watermark commit
```

If upload/verification fails, the watermark must not advance.

---

# MANUAL PIPELINE TEST

## 29. Test one study/entity without committing watermark

From the project root:

```bash
python - <<'PY'
from src.api.extract_all import ingest_study_entity

result = ingest_study_entity(
    study_id="ONC102",
    entity_name="adverse_event",
    run_id="manual_readme_test_001",
    load_date="2026-08-16",
    commit_watermark=False,
)

print(result.to_dict())
PY
```

This is useful for component debugging because it exercises extraction/parsing/validation/normalization/S3 while leaving the Airflow watermark unchanged.

Do not use a fixed manual `run_id` for normal Airflow execution; the DAG receives `context["run_id"]` from Airflow.

---

# S3 DEBUGGING

## 30. Verify the bucket from Python

```bash
python - <<'PY'
import os
import boto3

bucket = os.environ["S3_BUCKET_NAME"]
s3 = boto3.client("s3")

response = s3.list_objects_v2(
    Bucket=bucket,
    Prefix="act/raw/",
)

for obj in response.get("Contents", []):
    print(obj["Key"])
PY
```

---

## 31. Verify both study prefixes

```bash
python - <<'PY'
import os
import boto3

bucket = os.environ["S3_BUCKET_NAME"]
s3 = boto3.client("s3")

for study_id in ["ONC101", "ONC102"]:
    prefix = f"act/raw/study_id={study_id}/"

    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )

    objects = response.get("Contents", [])

    print("=" * 70)
    print(study_id, "objects:", len(objects))
    print("=" * 70)

    for obj in objects:
        print(obj["Key"])
PY
```

---

## 32. Verify AWS identity if AWS CLI is installed

Optional:

```bash
aws sts get-caller-identity
```

If the `aws` command is not installed, use the boto3 checks above instead.

---

# END-TO-END MULTI-STUDY TEST

## 33. Verify Rave source first

```bash
curl "http://127.0.0.1:8000/studies"
```

Expected studies:

```text
ONC101
ONC102
```

---

## 34. Trigger Airflow

```bash
airflow dags trigger act_rave_ingestion
```

---

## 35. Confirm DAG run

```bash
airflow dags list-runs act_rave_ingestion
```

Then inspect task states:

```bash
airflow tasks states-for-dag-run \
  act_rave_ingestion \
  "<RUN_ID>"
```

---

## 36. Confirm study isolation in S3

Expected:

```text
act/raw/study_id=ONC101/
act/raw/study_id=ONC102/
```

Each study should independently contain the eight entity prefixes when records exist for all eight entities.

---

## 37. Incremental-change test

Change one source record belonging only to ONC102, for example `AE10201`, and set:

```sql
updated_at = CURRENT_TIMESTAMP
```

Then trigger the DAG again:

```bash
airflow dags trigger act_rave_ingestion
```

Expected conceptually:

```text
ONC101
  ↓
uses ONC101 watermarks

ONC102
  ↓
uses ONC102 watermarks
  ↓
changed ONC102 AE is detected
```

Because of the configured overlap, the incremental run can re-read a small number of unchanged boundary records. That is expected and is not a failure of the watermark design.

---

# DEBUGGING PLAYBOOK

## 38. Airflow UI does not open

Check port 8080 locally:

```bash
curl -I http://127.0.0.1:8080
```

If it fails, verify that `airflow standalone` is still running.

If it succeeds but the browser does not open:

```text
VS Code / Codespaces
      ↓
PORTS
      ↓
8080
      ↓
Open in Browser
```

---

## 39. Airflow UI says 401 Invalid credentials

Read the generated local password:

```bash
cat $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated
```

Use the value stored for `admin`.

---

## 40. DAG is missing from the UI

```bash
airflow dags list-import-errors
```

Then:

```bash
airflow dags list
```

Check that `AIRFLOW_HOME` is correct:

```bash
echo $AIRFLOW_HOME
```

Check that the DAG exists:

```bash
ls -l /workspaces/act-data-platform/dags/act_pipeline_dag.py
```

---

## 41. DAG run stays queued

Check whether the DAG is paused:

```bash
airflow dags list | grep act_rave_ingestion
```

Unpause it:

```bash
airflow dags unpause -y act_rave_ingestion
```

Then inspect runs:

```bash
airflow dags list-runs act_rave_ingestion
```

---

## 42. Mapped task fails

First get the DAG run ID:

```bash
airflow dags list-runs act_rave_ingestion
```

Then inspect all task states:

```bash
airflow tasks states-for-dag-run \
  act_rave_ingestion \
  "<RUN_ID>"
```

Open the failed mapped task in the Airflow UI and read its task log from the first real exception/traceback line rather than only the final `Task failed` message.

The application logger format is designed to be captured by Airflow task logs.

---

## 43. Error mentions `logical_date`

The current DAG design should not derive S3 `load_date` from:

```python
context["logical_date"]
```

Manual Airflow runs can lack a logical date in situations where code assumes one exists.

The current design creates `load_date` once in the upstream work-item task and passes it into each mapped work item.

If an old copy of the DAG still contains `context["logical_date"]`, replace the old DAG with the current full version before testing again.

---

## 44. Rave API connection failure

Check the source API first:

```bash
curl -i http://127.0.0.1:8000/health
```

Then check the configured base URL:

```bash
python - <<'PY'
import os
print(os.getenv("RAVE_API_BASE_URL"))
PY
```

If FastAPI and Airflow run in the same Codespace, prefer:

```text
http://127.0.0.1:8000
```

---

## 45. API returns 429

The Rave client implements retry/backoff for rate limiting and transient failures.

Do not repeatedly trigger large manual tests while a DAG run is already running. Check:

```bash
airflow dags list-runs act_rave_ingestion --state running
```

Current DAG safety:

```text
max_active_runs = 1
```

---

## 46. Wrong response content type

The Rave client validates the actual response content type against the configured endpoint format.

Check the source directly:

```bash
curl -i "http://127.0.0.1:8000/adverse-events?study_id=ONC101"
```

Then compare with `config/endpoints.py`.

Expected formats:

```text
study                 JSON
site                  CSV
subject               JSON
visit                 XML
adverse_event         XML
lab_result            JSON
protocol_deviation    CSV
data_query            XML
```

---

## 47. Validation failure

The validator intentionally fails invalid batches instead of silently discarding bad records.

Typical checks include:

```text
required columns
required values
primary-key null/empty
primary-key duplicates
updated_at parseability
entity-specific domain checks
```

Read the exact validation exception in the mapped task log before changing the code.

---

## 48. No S3 object created

Check task logs in this order:

```text
API request succeeded?
        ↓
Parser succeeded?
        ↓
Validation succeeded?
        ↓
Normalization succeeded?
        ↓
DataFrame has records?
        ↓
S3 put_object succeeded?
        ↓
S3 head_object/checksum succeeded?
```

An empty incremental batch is valid and should not create an empty S3 object.

---

## 49. S3 object exists but watermark did not move

Check whether the S3 verification step succeeded.

The pipeline intentionally commits the watermark only after successful upload verification.

Read:

```bash
airflow variables get act_watermark__<STUDY>__<ENTITY>
```

and compare it with the maximum `updated_at` from the successful batch.

---

## 50. Duplicate-looking rows across different run folders

This can be expected.

Reasons:

```text
5-second watermark overlap
new DAG run_id per trigger
RAW preserves replay/overlap history
```

Retry-level idempotency is different:

```text
same DAG run + same study + same entity
        ↓
same deterministic S3 key
```

Cross-run deduplication belongs in downstream Snowflake transformation logic, not by deleting RAW history.

---

## 51. ONC102 is missing from S3

Check in this order:

```bash
curl "http://127.0.0.1:8000/studies"
```

Then:

```bash
curl "http://127.0.0.1:8000/adverse-events?study_id=ONC102"
```

Then trigger:

```bash
airflow dags trigger act_rave_ingestion
```

Then inspect run/task states:

```bash
airflow dags list-runs act_rave_ingestion
```

```bash
airflow tasks states-for-dag-run \
  act_rave_ingestion \
  "<RUN_ID>"
```

Finally inspect S3 with the Python listing command above.

---

# LOGGING

## 52. Logging standard

Application code uses Python logging rather than normal `print()` statements.

Expected logical format:

```text
timestamp | level | logger | message
```

Useful information in task logs includes:

```text
study_id
entity
load type: FULL / INCREMENTAL
page/record counts
S3 URI
checksum verification
previous watermark
candidate/new watermark
watermark commit status
```

Never log:

```text
AWS secret access keys
Codespaces tokens
passwords
sensitive clinical payloads unnecessarily
```

---

# GIT / CHANGE SAFETY

## 53. Before changing code

```bash
git status
git diff
```

After testing:

```bash
git add .
git status
git commit -m "Describe the change"
git push
```

Keep `.env`, Airflow database/runtime files, generated passwords, logs, and virtual environments out of Git.

When replacing a shared project file, use the **full current replacement file** so previously implemented entities and functionality are not accidentally removed.

---

# DAILY QUICK START

## 54. Terminal A — Rave API

```bash
cd /workspaces/act-rave-fastapi
source .venv/bin/activate
fastapi dev app/main.py --host 0.0.0.0 --port 8000
```

Verify:

```bash
curl -i http://127.0.0.1:8000/health
curl "http://127.0.0.1:8000/studies"
```

---

## 55. Terminal B — Airflow server

```bash
cd /workspaces/act-data-platform
source .venv/bin/activate
set -a
source .env
set +a
export AIRFLOW_HOME=/workspaces/act-data-platform/airflow
airflow standalone
```

---

## 56. Terminal C — Airflow commands

```bash
cd /workspaces/act-data-platform
source .venv/bin/activate
set -a
source .env
set +a
export AIRFLOW_HOME=/workspaces/act-data-platform/airflow
```

Then:

```bash
airflow dags list-import-errors
airflow dags unpause -y act_rave_ingestion
airflow dags trigger act_rave_ingestion
airflow dags list-runs act_rave_ingestion
```

---

# DAILY SHUTDOWN

## 57. Stop safely

```text
Airflow standalone terminal → CTRL+C
FastAPI terminal           → CTRL+C
```

PostgreSQL can remain running in the devcontainer unless you intentionally want to stop it.

Do not delete S3 RAW data or Airflow watermark Variables as part of a normal shutdown.

---

# QUICK DEBUG COMMAND SHEET

```bash
# -------------------------------
# SOURCE API
# -------------------------------
curl -i http://127.0.0.1:8000/health
curl "http://127.0.0.1:8000/studies"
curl "http://127.0.0.1:8000/adverse-events?study_id=ONC102"

# -------------------------------
# AIRFLOW
# -------------------------------
airflow version
airflow dags list
airflow dags list-import-errors
airflow dags details act_rave_ingestion
airflow dags unpause -y act_rave_ingestion
airflow dags trigger act_rave_ingestion
airflow dags list-runs act_rave_ingestion
airflow tasks list act_rave_ingestion

# -------------------------------
# WATERMARKS
# -------------------------------
airflow variables list | grep act_watermark
airflow variables list | grep ONC101
airflow variables list | grep ONC102
airflow variables get act_watermark__ONC101__adverse_event
airflow variables get act_watermark__ONC102__adverse_event

# -------------------------------
# AIRFLOW UI PASSWORD
# -------------------------------
cat $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated

# -------------------------------
# LOCAL PORTS
# -------------------------------
curl -I http://127.0.0.1:8080
curl -i http://127.0.0.1:8000/health
```

---

# NEXT PLATFORM PHASE

Do not start the Snowflake layer until the following are proven:

```text
✓ ONC101 lands in S3
✓ ONC102 lands in S3
✓ all 8 entities process correctly
✓ separate study/entity watermarks exist
✓ second-run incremental behavior works
✓ changed source data is detected
✓ failed uploads do not advance watermark
✓ retry-level S3 idempotency works
```

After those checks:

```text
Amazon S3 RAW
      ↓
Snowflake Stage / COPY ingestion
      ↓
Snowflake RAW tables
      ↓
dbt staging
      ↓
intermediate/business rules
      ↓
curated / marts
```

---

## Official references

- Apache Airflow 3.3 CLI reference: https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html
- Apache Airflow CLI usage: https://airflow.apache.org/docs/apache-airflow/stable/howto/usage-cli.html
- Apache Airflow standalone quick start: https://airflow.apache.org/docs/apache-airflow/stable/start.html
- GitHub Codespaces port forwarding: https://docs.github.com/en/codespaces/developing-in-a-codespace/forwarding-ports-in-your-codespace
- FastAPI CLI: https://fastapi.tiangolo.com/fastapi-cli/
