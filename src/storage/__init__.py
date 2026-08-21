# src/storage/__init__.py

"""
Storage layer for the ACT Data Platform.

The ingestion pipeline must not depend directly on a specific
storage technology.

Supported design:

    Airflow / ingestion
            |
            v
    StorageBackend
            |
      +-----+-----+
      |           |
      v           v
    Local        S3
   Storage     Storage
   (current)   (future)

Local filesystem storage is the default development backend.
An S3 implementation can be added later without changing the
API extraction, parsing, validation, or watermark logic.
"""
