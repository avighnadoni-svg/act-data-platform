# src/common/exceptions.py


# ============================================================
# BASE EXCEPTION
# ============================================================

class ACTPipelineError(Exception):
    """
    Base exception for all ACT data platform errors.
    """

    pass


# ============================================================
# CONFIGURATION ERRORS
# ============================================================

class ConfigurationError(ACTPipelineError):
    """
    Raised when required configuration is missing or invalid.

    Example:
    - RAVE_API_BASE_URL missing
    - S3_BUCKET_NAME missing
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
    Authentication failure.

    Example:
    HTTP 401
    """

    pass


class APIAuthorizationError(APIError):
    """
    Authorization failure.

    Example:
    HTTP 403
    """

    pass


class APIRateLimitError(APIError):
    """
    API rate limit exceeded.

    Example:
    HTTP 429
    """

    pass


class APITimeoutError(APIError):
    """
    API request timed out.
    """

    pass


class APIConnectionError(APIError):
    """
    Unable to connect to source API.
    """

    pass


class APIResponseError(APIError):
    """
    Source API returned an unexpected response.

    Example:
    HTTP 500
    unexpected status code
    """

    pass


# ============================================================
# FORMAT / PARSING ERRORS
# ============================================================

class UnsupportedFormatError(ACTPipelineError):
    """
    API returned unsupported content type.
    """

    pass


class ParsingError(ACTPipelineError):
    """
    Error while parsing JSON, XML or CSV response.
    """

    pass


class JSONParsingError(ParsingError):
    pass


class XMLParsingError(ParsingError):
    pass


class CSVParsingError(ParsingError):
    pass


# ============================================================
# DATA QUALITY ERRORS
# ============================================================

class DataValidationError(ACTPipelineError):
    """
    Parsed data failed validation.

    Example:
    - missing primary key
    - missing updated_at
    - invalid mandatory field
    """

    pass


class EmptyDataError(DataValidationError):
    """
    Raised when data was expected but no usable records exist.
    """

    pass


# ============================================================
# S3 ERRORS
# ============================================================

class S3Error(ACTPipelineError):
    """
    Base S3 exception.
    """

    pass


class S3UploadError(S3Error):
    """
    File could not be uploaded to S3.
    """

    pass


class S3ValidationError(S3Error):
    """
    Upload completed but S3 object could not be verified.
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
    Unable to read existing watermark.
    """

    pass


class WatermarkUpdateError(WatermarkError):
    """
    Unable to persist new watermark.
    """

    pass