# ACT dbt Project

The ACT dbt project transforms Snowflake RAW current-state tables into
analytics-ready staging, intermediate, and mart models.

## Data Flow

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