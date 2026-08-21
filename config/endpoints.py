# config/endpoints.py


ENDPOINTS = {


    # ========================================================
    # STUDY
    # ========================================================

    "study": {

        "path":
            "studies",

        "format":
            "json",

        "content_type":
            "application/json",

        "primary_key":
            "study_id",

        "watermark_field":
            "updated_at",

        "required_fields": [
            "study_id",
            "study_name",
            "updated_at",
        ],

        "storage_prefix":
            "study",
    },


    # ========================================================
    # SITE
    # ========================================================

    "site": {

        "path":
            "sites",

        "format":
            "csv",

        "content_type":
            "text/csv",

        "primary_key":
            "site_id",

        "watermark_field":
            "updated_at",

        "required_fields": [
            "site_id",
            "study_id",
            "updated_at",
        ],

        "storage_prefix":
            "site",
    },


    # ========================================================
    # SUBJECT
    # ========================================================

    "subject": {

        "path":
            "subjects",

        "format":
            "json",

        "content_type":
            "application/json",

        "primary_key":
            "subject_id",

        "watermark_field":
            "updated_at",

        "required_fields": [
            "subject_id",
            "study_id",
            "site_id",
            "updated_at",
        ],

        "storage_prefix":
            "subject",
    },


    # ========================================================
    # VISIT
    # ========================================================

    "visit": {

        "path":
            "visits",

        "format":
            "xml",

        "content_type":
            "application/xml",

        "primary_key":
            "visit_id",

        "watermark_field":
            "updated_at",

        "required_fields": [
            "visit_id",
            "study_id",
            "subject_id",
            "updated_at",
        ],

        "storage_prefix":
            "visit",
    },


    # ========================================================
    # ADVERSE EVENT
    # ========================================================

    "adverse_event": {

        "path":
            "adverse-events",

        "format":
            "xml",

        "content_type":
            "application/xml",

        "primary_key":
            "ae_id",

        "watermark_field":
            "updated_at",

        "required_fields": [
            "ae_id",
            "study_id",
            "subject_id",
            "event_term",
            "updated_at",
        ],

        "storage_prefix":
            "adverse_event",
    },


    # ========================================================
    # LAB RESULT
    # ========================================================

    "lab_result": {

        "path":
            "lab-results",

        "format":
            "json",

        "content_type":
            "application/json",

        "primary_key":
            "lab_id",

        "watermark_field":
            "updated_at",

        "required_fields": [
            "lab_id",
            "study_id",
            "subject_id",
            "test_name",
            "updated_at",
        ],

        "storage_prefix":
            "lab_result",
    },


    # ========================================================
    # PROTOCOL DEVIATION
    # ========================================================

    "protocol_deviation": {

        "path":
            "protocol-deviations",

        "format":
            "csv",

        "content_type":
            "text/csv",

        "primary_key":
            "deviation_id",

        "watermark_field":
            "updated_at",

        "required_fields": [
            "deviation_id",
            "study_id",
            "subject_id",
            "site_id",
            "updated_at",
        ],

        "storage_prefix":
            "protocol_deviation",
    },


    # ========================================================
    # DATA QUERY
    # ========================================================

    "data_query": {

        "path":
            "data-queries",

        "format":
            "xml",

        "content_type":
            "application/xml",

        "primary_key":
            "query_id",

        "watermark_field":
            "updated_at",

        "required_fields": [
            "query_id",
            "study_id",
            "subject_id",
            "site_id",
            "updated_at",
        ],

        "storage_prefix":
            "data_query",
    },
}


# ============================================================
# COMMON SETTINGS
# ============================================================

DEFAULT_PAGE_SIZE = 100

API_TIMEOUT_SECONDS = 30

MAX_API_RETRIES = 3