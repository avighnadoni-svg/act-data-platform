# config/endpoints.py


# ============================================================
# ACT RAVE SOURCE ENDPOINT CONFIGURATION
# ============================================================
#
# path
#     FastAPI endpoint
#
# format
#     Expected source format
#
# content_type
#     Expected HTTP Content-Type
#
# primary_key
#     Used later for validation
#
# watermark_field
#     Field used for incremental extraction
#
# s3_prefix
#     Landing path in S3
#
# ============================================================


ENDPOINTS = {

    # --------------------------------------------------------
    # STUDY
    # Nested JSON
    # --------------------------------------------------------

    "study": {
        "path": "studies",
        "format": "json",
        "content_type": "application/json",
        "primary_key": "study_id",
        "watermark_field": "updated_at",
        "s3_prefix": "study",
    },


    # --------------------------------------------------------
    # SITE
    # CSV
    # --------------------------------------------------------

    "site": {
        "path": "sites",
        "format": "csv",
        "content_type": "text/csv",
        "primary_key": "site_id",
        "watermark_field": "updated_at",
        "s3_prefix": "site",
    },


    # --------------------------------------------------------
    # SUBJECT
    # Nested JSON
    # --------------------------------------------------------

    "subject": {
        "path": "subjects",
        "format": "json",
        "content_type": "application/json",
        "primary_key": "subject_id",
        "watermark_field": "updated_at",
        "s3_prefix": "subject",
    },


    # --------------------------------------------------------
    # VISIT
    # XML
    # --------------------------------------------------------

    "visit": {
        "path": "visits",
        "format": "xml",
        "content_type": "application/xml",
        "primary_key": "visit_id",
        "watermark_field": "updated_at",
        "s3_prefix": "visit",
    },


    # --------------------------------------------------------
    # ADVERSE EVENT
    # Complex XML
    # --------------------------------------------------------

    "adverse_event": {
        "path": "adverse-events",
        "format": "xml",
        "content_type": "application/xml",
        "primary_key": "ae_id",
        "watermark_field": "updated_at",
        "s3_prefix": "adverse_event",
    },


    # --------------------------------------------------------
    # LAB RESULT
    # Nested JSON
    # --------------------------------------------------------

    "lab_result": {
        "path": "lab-results",
        "format": "json",
        "content_type": "application/json",
        "primary_key": "lab_id",
        "watermark_field": "updated_at",
        "s3_prefix": "lab_result",
    },


    # --------------------------------------------------------
    # PROTOCOL DEVIATION
    # CSV
    # --------------------------------------------------------

    "protocol_deviation": {
        "path": "protocol-deviations",
        "format": "csv",
        "content_type": "text/csv",
        "primary_key": "deviation_id",
        "watermark_field": "updated_at",
        "s3_prefix": "protocol_deviation",
    },


    # --------------------------------------------------------
    # DATA QUERY
    # XML
    # --------------------------------------------------------

    "data_query": {
        "path": "data-queries",
        "format": "xml",
        "content_type": "application/xml",
        "primary_key": "query_id",
        "watermark_field": "updated_at",
        "s3_prefix": "data_query",
    },
}


# ============================================================
# COMMON SETTINGS
# ============================================================

DEFAULT_PAGE_SIZE = 100

API_TIMEOUT_SECONDS = 30

MAX_API_RETRIES = 3