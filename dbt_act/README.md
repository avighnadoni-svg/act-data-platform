# ACT dbt Project

This dbt project transforms the ACT Snowflake RAW current-state tables into
staging, intermediate, and mart models.

## Layer flow

```text
ACT_DB.RAW.RAW_*_CURRENT
        |
        v
dbt staging
        |
        v
dbt intermediate
        |
        v
dbt marts
```

## Local setup

1. Activate the existing Python virtual environment.
2. Install application dependencies from the repository root:

```bash
pip install -r requirements-app.txt
```

3. Load the repository `.env` into the current shell:

```bash
cd /workspaces/act-data-platform
set -a
source .env
set +a
```

4. Create the dbt profile directory:

```bash
mkdir -p ~/.dbt
```

5. Copy the example profile:

```bash
cp dbt_act/profiles.yml.example ~/.dbt/profiles.yml
```

6. Validate the connection:

```bash
cd dbt_act
dbt debug
```

7. Validate the project:

```bash
dbt parse
```

## Development schemas

With the default development profile:

```text
DBT_DEV_STAGING
DBT_DEV_INTERMEDIATE
DBT_DEV_MARTS
```

The base schema can be changed with:

```text
DBT_DEV_SCHEMA
```

No passwords or secrets are stored inside the dbt project files.
