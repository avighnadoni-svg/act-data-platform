# src/common/exceptions.py


# ============================================================
# BASE EXCEPTION
# ============================================================

class ACTPipelineError(Exception):
    """
    Base exception for all ACT Data Platform errors.
    """

    pass


# ============================================================
# CONFIGURATION ERRORS
# ============================================================

class ConfigurationError(ACTPipelineError):
    """
    Raised when required application configuration
    is missing or invalid.

    Examples:

    - RAVE_API_BASE_URL missing
    - STORAGE_BACKEND invalid
    - LOCAL_STORAGE_ROOT missing
    """

    pass


# ============================================================
# API ERRORS
# ============================================================

class APIError(ACTPipelineError):
    """
    Base exception for source API errors.
    """

    pass


class APIAuthenticationError(APIError):
    """
    Source API authentication failure.

    Example:

        HTTP 401
    """

    pass


class APIAuthorizationError(APIError):
    """
    Source API authorization failure.

    Example:

        HTTP 403
    """

    pass


class APIRateLimitError(APIError):
    """
    Source API rate limit exceeded.

    Example:

        HTTP 429
    """

    pass


class APITimeoutError(APIError):
    """
    Source API request timed out.
    """

    pass


class APIConnectionError(APIError):
    """
    Unable to connect to the source API.
    """

    pass


class APIResponseError(APIError):
    """
    Source API returned an unexpected response.

    Examples:

    - HTTP 500
    - unexpected status code
    - invalid response metadata
    """

    pass


# ============================================================
# FORMAT / PARSING ERRORS
# ============================================================

class UnsupportedFormatError(ACTPipelineError):
    """
    API returned an unsupported content type.
    """

    pass


class ParsingError(ACTPipelineError):
    """
    Base parsing error.
    """

    pass


class JSONParsingError(ParsingError):
    """
    JSON parsing failed.
    """

    pass


class XMLParsingError(ParsingError):
    """
    XML parsing failed.
    """

    pass


class CSVParsingError(ParsingError):
    """
    CSV parsing failed.
    """

    pass


# ============================================================
# DATA QUALITY ERRORS
# ============================================================

class DataValidationError(ACTPipelineError):
    """
    Parsed or normalized data failed validation.

    Examples:

    - missing primary key
    - missing updated_at
    - missing study_id
    - multiple studies in one storage partition
    """

    pass


class EmptyDataError(DataValidationError):
    """
    Raised when data was expected but no usable
    records were returned.
    """

    pass


# ============================================================
# GENERIC STORAGE ERRORS
# ============================================================

class StorageError(ACTPipelineError):
    """
    Base exception for all storage backends.

    Examples:

    - local filesystem
    - S3
    - Azure Blob Storage
    """

    pass


class StorageWriteError(StorageError):
    """
    Data could not be persisted to the configured
    storage backend.
    """

    pass


class StorageValidationError(StorageError):
    """
    Data was written but verification failed.

    Examples:

    - file does not exist
    - file size mismatch
    - checksum mismatch
    """

    pass


# ============================================================
# LEGACY S3 ERRORS
# ============================================================
#
# Keep these temporarily while the existing S3 implementation
# is still present in the repository.
#
# Once all imports have been migrated to the generic storage
# layer, src/aws can be removed completely.
# ============================================================

class S3Error(StorageError):
    """
    Legacy base S3 exception.

    Retained temporarily for migration compatibility.
    """

    pass


class S3UploadError(S3Error, StorageWriteError):
    """
    Legacy S3 upload exception.

    Retained temporarily so the old S3 client continues
    importing successfully during the migration.
    """

    pass


class S3ValidationError(S3Error, StorageValidationError):
    """
    Legacy S3 validation exception.

    Retained temporarily so the old S3 client continues
    importing successfully during the migration.
    """

    pass


# ============================================================
# WATERMARK ERRORS
# ============================================================

class WatermarkError(ACTPipelineError):
    """
    Base watermark exception.
    """

    pass


class WatermarkReadError(WatermarkError):
    """
    Unable to read the existing watermark.
    """

    pass


class WatermarkUpdateError(WatermarkError):
    """
    Unable to persist a new watermark.
    """

    pass