# src/storage/storage_factory.py

import os

from src.common.exceptions import ConfigurationError

from src.storage.storage_backend import StorageBackend
from src.storage.local_storage import LocalStorageBackend


# ============================================================
# SUPPORTED STORAGE BACKENDS
# ============================================================

LOCAL_BACKEND = "local"
S3_BACKEND = "s3"


# ============================================================
# GET STORAGE BACKEND
# ============================================================

def get_storage_backend() -> StorageBackend:
    """
    Return the configured ACT storage backend.

    Environment variable:

        STORAGE_BACKEND

    Current supported backend:

        local

    Future backend:

        s3

    Default:

        local

    Example:

        STORAGE_BACKEND=local

    The rest of the ingestion pipeline should call only:

        storage = get_storage_backend()

    and should not instantiate storage implementations
    directly.
    """

    backend_name = (
        os.getenv(
            "STORAGE_BACKEND",
            LOCAL_BACKEND,
        )
        .strip()
        .lower()
    )


    # ========================================================
    # LOCAL FILESYSTEM
    # ========================================================

    if backend_name == LOCAL_BACKEND:

        return LocalStorageBackend()


    # ========================================================
    # FUTURE AWS S3
    # ========================================================

    if backend_name == S3_BACKEND:

        try:

            from src.storage.s3_storage import (
                S3StorageBackend,
            )

        except ImportError as exc:

            raise ConfigurationError(
                (
                    "STORAGE_BACKEND=s3 was requested, "
                    "but S3StorageBackend is not currently "
                    "installed in this local-development "
                    "version of the ACT Data Platform."
                )
            ) from exc


        return S3StorageBackend()


    # ========================================================
    # UNKNOWN BACKEND
    # ========================================================

    raise ConfigurationError(
        (
            "Unsupported STORAGE_BACKEND="
            f"{backend_name}. "
            "Supported values: "
            "local, s3"
        )
    )