from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


# ============================================================
# STORAGE WRITE RESULT
# ============================================================

@dataclass(frozen=True)
class StorageWriteResult:
    """
    Metadata returned after successfully writing
    one ACT entity dataset to a storage backend.

    This object is intentionally storage-neutral.

    Examples of storage backends:

        local
        s3
    """

    entity_name: str
    study_id: str

    stored: bool
    record_count: int

    storage_backend: str

    storage_path: str | None
    storage_uri: str | None

    checksum: str | None
    file_size_bytes: int

    run_id: str
    load_date: str


# ============================================================
# STORAGE BACKEND CONTRACT
# ============================================================

class StorageBackend(ABC):
    """
    Common interface for ACT storage implementations.

    The ingestion pipeline should depend on this class
    instead of depending directly on:

        local filesystem
        AWS S3
        Azure Blob
        or another storage technology

    Current implementation:

        LocalStorageBackend

    Future implementation:

        S3StorageBackend
    """

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """
        Return a short identifier for the backend.

        Examples:

            local
            s3
        """

        raise NotImplementedError


    @abstractmethod
    def write_dataframe(
        self,
        entity_name: str,
        study_id: str,
        dataframe: pd.DataFrame,
        run_id: str,
        load_date: str,
    ) -> StorageWriteResult:
        """
        Persist one normalized Pandas DataFrame.

        Every backend must preserve the logical ACT
        partition structure:

            study_id=<study_id>/
            <entity>/
            load_date=<YYYY-MM-DD>/
            run_id=<run_id>/
            <entity>.csv

        The physical location is decided by the
        concrete backend.

        Example local path:

            data/raw/
            study_id=ONC101/
            adverse_event/
            load_date=2026-08-20/
            run_id=manual_test_001/
            adverse_event.csv

        Example future S3 object:

            s3://bucket/act/raw/
            study_id=ONC101/
            adverse_event/
            load_date=2026-08-20/
            run_id=manual_test_001/
            adverse_event.csv
        """

        raise NotImplementedError
