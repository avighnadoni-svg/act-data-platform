-- ============================================================================
-- ACT Data Platform - S3 Storage Integration
-- File: snowflake/sql/002_create_storage_integration.sql
-- Purpose: Allow Snowflake to access the ACT S3 raw landing area through
--          an AWS IAM role. The AWS role ARN is supplied at execution time.
--
-- Execute only after the AWS IAM role has been created.
-- Example:
-- snow sql -c act_dev \
--   -f snowflake/sql/002_create_storage_integration.sql \
--   -D "aws_role_arn=arn:aws:iam::123456789012:role/act-snowflake-s3-role"
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION ACT_S3_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<% aws_role_arn %>'
    STORAGE_ALLOWED_LOCATIONS = (
        's3://act-clinical-data-dev/act/raw/'
    );

-- These values are required when configuring the AWS IAM role trust policy.
DESC INTEGRATION ACT_S3_INT;
