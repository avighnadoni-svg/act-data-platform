# src/api/rave_client.py

import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from config.endpoints import (
    ENDPOINTS,
    API_TIMEOUT_SECONDS,
    MAX_API_RETRIES,
    DEFAULT_PAGE_SIZE,
)

from src.common.exceptions import (
    ConfigurationError,
    APIAuthenticationError,
    APIAuthorizationError,
    APIRateLimitError,
    APITimeoutError,
    APIConnectionError,
    APIResponseError,
    UnsupportedFormatError,
)

from src.common.logging_config import get_logger


logger = get_logger(__name__)


# ============================================================
# API RESPONSE OBJECT
# ============================================================

@dataclass
class APIResponse:
    """
    Standard response object returned by the Rave client.

    We intentionally keep the payload raw here.

    Parsing JSON / XML / CSV will happen later
    inside the parser layer.
    """

    entity_name: str
    endpoint: str
    status_code: int
    content_type: str
    text: str
    headers: dict[str, str]
    offset: int
    limit: int
    updated_since: str | None


# ============================================================
# CONFIGURATION
# ============================================================
#
# Airflow task processes must be able to resolve the project
# configuration even when the shell that launches the task does
# not expose the repository .env file.
#
# Environment variables still take precedence because
# override=False.
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

ENV_FILE = (
    PROJECT_ROOT
    / ".env"
)

load_dotenv(
    dotenv_path=ENV_FILE,
    override=False,
)


def _get_rave_api_base_url() -> str | None:
    """
    Resolve the Rave API base URL at runtime.

    Runtime lookup avoids keeping a stale import-time value.
    """

    value = os.getenv(
        "RAVE_API_BASE_URL"
    )

    if value:
        return value.strip()

    return None


def _get_rave_codespace_token() -> str | None:
    """
    Resolve the optional GitHub Codespaces token at runtime.
    """

    value = os.getenv(
        "RAVE_CODESPACE_TOKEN"
    )

    if value:
        return value.strip()

    return None


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def _validate_configuration() -> None:
    """
    Validate required API configuration.
    """

    rave_api_base_url = (
        _get_rave_api_base_url()
    )

    if not rave_api_base_url:

        raise ConfigurationError(
            (
                "RAVE_API_BASE_URL is missing. "
                f"Checked environment and {ENV_FILE}"
            )
        )


def _normalize_content_type(
    content_type: str
) -> str:
    """
    Convert:

        application/json; charset=utf-8

    into:

        application/json
    """

    if not content_type:
        return ""

    return (
        content_type
        .split(";")[0]
        .strip()
        .lower()
    )


def _calculate_retry_delay(
    attempt: int,
    retry_after: str | None = None,
) -> float:
    """
    Determine how long to wait before retrying.

    Priority:

    1. Retry-After header from API
    2. Exponential backoff
    3. Small random jitter
    """

    # --------------------------------------------------------
    # Retry-After can be:
    #
    # Retry-After: 5
    #
    # OR an HTTP date.
    # --------------------------------------------------------

    if retry_after:

        # Retry-After as seconds

        try:

            return max(
                float(retry_after),
                0.0,
            )

        except ValueError:
            pass

        # Retry-After as HTTP date

        try:

            retry_datetime = parsedate_to_datetime(
                retry_after
            )

            if retry_datetime.tzinfo is None:

                retry_datetime = (
                    retry_datetime.replace(
                        tzinfo=timezone.utc
                    )
                )

            now = datetime.now(
                timezone.utc
            )

            delay = (
                retry_datetime - now
            ).total_seconds()

            return max(
                delay,
                0.0,
            )

        except Exception:
            pass

    # --------------------------------------------------------
    # Exponential backoff
    #
    # Attempt 1 → ~1 second
    # Attempt 2 → ~2 seconds
    # Attempt 3 → ~4 seconds
    # --------------------------------------------------------

    base_delay = 2 ** (
        attempt - 1
    )

    # Small jitter prevents multiple clients
    # retrying at exactly the same time.

    jitter = random.uniform(
        0,
        0.5,
    )

    return base_delay + jitter


# ============================================================
# RAVE API CLIENT
# ============================================================

class RaveAPIClient:
    """
    HTTP client for ACT Rave source APIs.

    Responsibilities:

    - authentication headers
    - timeout
    - retry
    - 401 / 403 handling
    - 429 handling
    - 5xx handling
    - Content-Type validation
    - updated_since
    - offset / limit pagination parameters

    Parsing is NOT done here.
    """

    def __init__(self):

        _validate_configuration()

        rave_api_base_url = (
            _get_rave_api_base_url()
        )

        if not rave_api_base_url:
            raise ConfigurationError(
                "RAVE_API_BASE_URL could not be resolved"
            )

        self.base_url = (
            rave_api_base_url.rstrip("/")
        )

        rave_codespace_token = (
            _get_rave_codespace_token()
        )

        self.session = requests.Session()

        logger.info(
            (
                "Rave API client configuration resolved "
                "base_url=%s "
                "env_file=%s "
                "codespace_token_configured=%s"
            ),
            self.base_url,
            ENV_FILE,
            bool(rave_codespace_token),
        )

        # ----------------------------------------------------
        # Default headers
        # ----------------------------------------------------

        self.session.headers.update(
            {
                "Accept": "*/*",
                "User-Agent":
                    "ACT-Data-Platform/1.0",
            }
        )

        # ----------------------------------------------------
        # GitHub Codespaces authentication
        #
        # Optional.
        #
        # If the FastAPI forwarded port is Public,
        # this token is not required.
        #
        # If it is Private, GitHub uses this header.
        # ----------------------------------------------------

        if rave_codespace_token:

            self.session.headers.update(
                {
                    "X-Github-Token":
                        rave_codespace_token
                }
            )

            logger.info(
                "Rave API authentication token configured"
            )

        else:

            logger.info(
                "Rave API authentication token not configured"
            )


    # ========================================================
    # URL
    # ========================================================

    def _build_url(
        self,
        path: str,
    ) -> str:

        return (
            f"{self.base_url}/"
            f"{path.lstrip('/')}"
        )


    # ========================================================
    # RESPONSE VALIDATION
    # ========================================================

    def _validate_content_type(
        self,
        entity_name: str,
        response: requests.Response,
    ) -> str:
        """
        Validate actual API Content-Type against
        endpoint configuration.
        """

        expected = (
            ENDPOINTS[
                entity_name
            ]["content_type"]
            .lower()
        )

        actual = (
            _normalize_content_type(
                response.headers.get(
                    "Content-Type",
                    ""
                )
            )
        )

        if actual != expected:

            logger.error(
                (
                    "entity=%s "
                    "content_type_mismatch "
                    "expected=%s "
                    "actual=%s"
                ),
                entity_name,
                expected,
                actual,
            )

            raise UnsupportedFormatError(
                (
                    f"Unexpected Content-Type "
                    f"for entity={entity_name}. "
                    f"Expected={expected}, "
                    f"Actual={actual}"
                )
            )

        return actual


    # ========================================================
    # GET ONE PAGE
    # ========================================================

    def get_page(
        self,
        entity_name: str,
        updated_since: str | None = None,
        offset: int = 0,
        limit: int = DEFAULT_PAGE_SIZE,
        extra_params: dict[str, Any] | None = None,
    ) -> APIResponse:
        """
        Fetch one page from a configured API endpoint.

        Example:

        get_page(
            entity_name="adverse_event",
            updated_since="2026-08-16T08:00:00Z",
            offset=0,
            limit=100
        )
        """

        if entity_name not in ENDPOINTS:

            raise ConfigurationError(
                (
                    f"Unknown entity_name="
                    f"{entity_name}"
                )
            )

        config = ENDPOINTS[
            entity_name
        ]

        endpoint = config["path"]

        url = self._build_url(
            endpoint
        )

        # ----------------------------------------------------
        # Query Parameters
        # ----------------------------------------------------

        params: dict[str, Any] = {
            "offset": offset,
            "limit": limit,
        }

        if updated_since:

            params[
                "updated_since"
            ] = updated_since

        if extra_params:

            params.update(
                extra_params
            )

        logger.info(
            (
                "entity=%s "
                "api_request_started "
                "endpoint=%s "
                "offset=%s "
                "limit=%s "
                "updated_since=%s"
            ),
            entity_name,
            endpoint,
            offset,
            limit,
            updated_since,
        )

        # ----------------------------------------------------
        # Retry loop
        # ----------------------------------------------------

        for attempt in range(
            1,
            MAX_API_RETRIES + 1,
        ):

            try:

                logger.info(
                    (
                        "entity=%s "
                        "api_attempt=%s/%s"
                    ),
                    entity_name,
                    attempt,
                    MAX_API_RETRIES,
                )

                response = self.session.get(
                    url,
                    params=params,

                    # Explicit timeout is important.
                    timeout=API_TIMEOUT_SECONDS,
                )

                status_code = (
                    response.status_code
                )

                logger.info(
                    (
                        "entity=%s "
                        "api_response_received "
                        "status_code=%s "
                        "attempt=%s"
                    ),
                    entity_name,
                    status_code,
                    attempt,
                )


                # =================================================
                # SUCCESS
                # =================================================

                if status_code == 200:

                    content_type = (
                        self._validate_content_type(
                            entity_name,
                            response,
                        )
                    )

                    logger.info(
                        (
                            "entity=%s "
                            "api_request_success "
                            "content_type=%s "
                            "response_bytes=%s"
                        ),
                        entity_name,
                        content_type,
                        len(response.content),
                    )

                    return APIResponse(
                        entity_name=entity_name,
                        endpoint=endpoint,
                        status_code=status_code,
                        content_type=content_type,
                        text=response.text,
                        headers=dict(
                            response.headers
                        ),
                        offset=offset,
                        limit=limit,
                        updated_since=updated_since,
                    )


                # =================================================
                # 401
                # =================================================

                if status_code == 401:

                    logger.error(
                        (
                            "entity=%s "
                            "authentication_failed "
                            "status_code=401"
                        ),
                        entity_name,
                    )

                    raise APIAuthenticationError(
                        (
                            f"Authentication failed "
                            f"for entity={entity_name}"
                        )
                    )


                # =================================================
                # 403
                # =================================================

                if status_code == 403:

                    logger.error(
                        (
                            "entity=%s "
                            "authorization_failed "
                            "status_code=403"
                        ),
                        entity_name,
                    )

                    raise APIAuthorizationError(
                        (
                            f"Authorization failed "
                            f"for entity={entity_name}"
                        )
                    )


                # =================================================
                # 429 RATE LIMIT
                # =================================================

                if status_code == 429:

                    retry_after = (
                        response.headers.get(
                            "Retry-After"
                        )
                    )

                    if attempt >= MAX_API_RETRIES:

                        raise APIRateLimitError(
                            (
                                f"API rate limit exceeded "
                                f"after "
                                f"{MAX_API_RETRIES} "
                                f"attempts for "
                                f"entity={entity_name}"
                            )
                        )

                    delay = (
                        _calculate_retry_delay(
                            attempt=attempt,
                            retry_after=retry_after,
                        )
                    )

                    logger.warning(
                        (
                            "entity=%s "
                            "rate_limit_received "
                            "attempt=%s "
                            "retry_after_seconds=%.2f"
                        ),
                        entity_name,
                        attempt,
                        delay,
                    )

                    time.sleep(
                        delay
                    )

                    continue


                # =================================================
                # SERVER ERRORS
                #
                # 500
                # 502
                # 503
                # 504
                # etc.
                # =================================================

                if 500 <= status_code < 600:

                    if attempt >= MAX_API_RETRIES:

                        raise APIResponseError(
                            (
                                f"Source API returned "
                                f"HTTP {status_code} "
                                f"after "
                                f"{MAX_API_RETRIES} "
                                f"attempts "
                                f"for entity="
                                f"{entity_name}"
                            )
                        )

                    delay = (
                        _calculate_retry_delay(
                            attempt
                        )
                    )

                    logger.warning(
                        (
                            "entity=%s "
                            "server_error "
                            "status_code=%s "
                            "attempt=%s "
                            "retry_in_seconds=%.2f"
                        ),
                        entity_name,
                        status_code,
                        attempt,
                        delay,
                    )

                    time.sleep(
                        delay
                    )

                    continue


                # =================================================
                # OTHER HTTP ERRORS
                #
                # Example:
                #
                # 400
                # 404
                # 422
                #
                # These generally should NOT be retried.
                # =================================================

                logger.error(
                    (
                        "entity=%s "
                        "unexpected_http_status "
                        "status_code=%s "
                        "response=%s"
                    ),
                    entity_name,
                    status_code,

                    # Limit response logging.
                    response.text[:500],
                )

                raise APIResponseError(
                    (
                        f"API request failed "
                        f"for entity={entity_name}. "
                        f"HTTP status="
                        f"{status_code}"
                    )
                )


            # =====================================================
            # TIMEOUT
            # =====================================================

            except requests.exceptions.Timeout as exc:

                if attempt >= MAX_API_RETRIES:

                    logger.exception(
                        (
                            "entity=%s "
                            "api_timeout "
                            "attempts_exhausted"
                        ),
                        entity_name,
                    )

                    raise APITimeoutError(
                        (
                            f"API timeout after "
                            f"{MAX_API_RETRIES} "
                            f"attempts for "
                            f"entity={entity_name}"
                        )
                    ) from exc

                delay = (
                    _calculate_retry_delay(
                        attempt
                    )
                )

                logger.warning(
                    (
                        "entity=%s "
                        "api_timeout "
                        "attempt=%s "
                        "retry_in_seconds=%.2f"
                    ),
                    entity_name,
                    attempt,
                    delay,
                )

                time.sleep(
                    delay
                )


            # =====================================================
            # CONNECTION ERROR
            # =====================================================

            except requests.exceptions.ConnectionError as exc:

                if attempt >= MAX_API_RETRIES:

                    logger.exception(
                        (
                            "entity=%s "
                            "api_connection_error "
                            "attempts_exhausted"
                        ),
                        entity_name,
                    )

                    raise APIConnectionError(
                        (
                            f"Unable to connect "
                            f"to API after "
                            f"{MAX_API_RETRIES} "
                            f"attempts for "
                            f"entity={entity_name}"
                        )
                    ) from exc

                delay = (
                    _calculate_retry_delay(
                        attempt
                    )
                )

                logger.warning(
                    (
                        "entity=%s "
                        "api_connection_error "
                        "attempt=%s "
                        "retry_in_seconds=%.2f"
                    ),
                    entity_name,
                    attempt,
                    delay,
                )

                time.sleep(
                    delay
                )


            # =====================================================
            # CUSTOM PIPELINE EXCEPTIONS
            #
            # Do NOT wrap these again.
            # =====================================================

            except (
                APIAuthenticationError,
                APIAuthorizationError,
                APIRateLimitError,
                APIResponseError,
                UnsupportedFormatError,
            ):
                raise


            # =====================================================
            # OTHER REQUESTS ERRORS
            # =====================================================

            except requests.exceptions.RequestException as exc:

                logger.exception(
                    (
                        "entity=%s "
                        "unexpected_request_error"
                    ),
                    entity_name,
                )

                raise APIResponseError(
                    (
                        f"Unexpected HTTP request "
                        f"failure for "
                        f"entity={entity_name}"
                    )
                ) from exc


        # Defensive fallback.
        # Normally execution should never reach this point.

        raise APIResponseError(
            (
                f"API request failed "
                f"for entity={entity_name}"
            )
        )


    # ========================================================
    # CLOSE SESSION
    # ========================================================

    def close(self) -> None:

        self.session.close()

        logger.info(
            "Rave API client session closed"
        )


    # ========================================================
    # CONTEXT MANAGER
    # ========================================================

    def __enter__(self):

        return self


    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ):

        self.close()