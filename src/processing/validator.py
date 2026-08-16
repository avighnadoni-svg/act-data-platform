# src/processing/validator.py

from dataclasses import dataclass

import pandas as pd

from config.endpoints import ENDPOINTS

from src.common.exceptions import (
    ConfigurationError,
    DataValidationError,
)

from src.common.logging_config import get_logger


logger = get_logger(__name__)


# ============================================================
# VALIDATION RESULT
# ============================================================

@dataclass
class ValidationResult:
    """
    Result returned after successful validation.
    """

    entity_name: str

    dataframe: pd.DataFrame

    record_count: int

    primary_key: str

    watermark_field: str

    max_watermark: str | None


# ============================================================
# HELPERS
# ============================================================

def _validate_entity_configuration(
    entity_name: str,
) -> dict:
    """
    Verify entity exists in endpoint configuration.
    """

    if entity_name not in ENDPOINTS:

        raise ConfigurationError(
            f"Unknown entity={entity_name}"
        )

    return ENDPOINTS[entity_name]


def _validate_required_columns(
    entity_name: str,
    df: pd.DataFrame,
    required_fields: list[str],
) -> None:
    """
    Ensure all required columns exist.
    """

    missing_columns = [
        column
        for column in required_fields
        if column not in df.columns
    ]

    if missing_columns:

        logger.error(
            (
                "entity=%s "
                "missing_required_columns=%s"
            ),
            entity_name,
            missing_columns,
        )

        raise DataValidationError(
            (
                f"Missing required columns "
                f"for entity={entity_name}: "
                f"{missing_columns}"
            )
        )


def _validate_primary_key(
    entity_name: str,
    df: pd.DataFrame,
    primary_key: str,
) -> None:
    """
    Primary key must:

    - exist
    - not contain NULL
    - not contain empty strings
    - not contain duplicates
    """

    # --------------------------------------------------------
    # NULL CHECK
    # --------------------------------------------------------

    null_mask = (
        df[primary_key]
        .isna()
    )

    null_count = int(
        null_mask.sum()
    )

    if null_count > 0:

        logger.error(
            (
                "entity=%s "
                "primary_key=%s "
                "null_count=%s"
            ),
            entity_name,
            primary_key,
            null_count,
        )

        raise DataValidationError(
            (
                f"Primary key {primary_key} "
                f"contains {null_count} NULL values "
                f"for entity={entity_name}"
            )
        )

    # --------------------------------------------------------
    # EMPTY STRING CHECK
    # --------------------------------------------------------

    empty_mask = (
        df[primary_key]
        .astype(str)
        .str.strip()
        .eq("")
    )

    empty_count = int(
        empty_mask.sum()
    )

    if empty_count > 0:

        raise DataValidationError(
            (
                f"Primary key {primary_key} "
                f"contains {empty_count} empty values "
                f"for entity={entity_name}"
            )
        )

    # --------------------------------------------------------
    # DUPLICATE CHECK
    # --------------------------------------------------------

    duplicate_mask = (
        df.duplicated(
            subset=[primary_key],
            keep=False,
        )
    )

    duplicate_count = int(
        duplicate_mask.sum()
    )

    if duplicate_count > 0:

        duplicate_values = (
            df.loc[
                duplicate_mask,
                primary_key,
            ]
            .astype(str)
            .unique()
            .tolist()
        )

        logger.error(
            (
                "entity=%s "
                "duplicate_primary_keys=%s"
            ),
            entity_name,
            duplicate_values,
        )

        raise DataValidationError(
            (
                f"Duplicate primary keys found "
                f"for entity={entity_name}: "
                f"{duplicate_values}"
            )
        )


def _validate_required_values(
    entity_name: str,
    df: pd.DataFrame,
    required_fields: list[str],
) -> None:
    """
    Required fields cannot contain NULL values.
    """

    for column in required_fields:

        null_count = int(
            df[column]
            .isna()
            .sum()
        )

        if null_count > 0:

            logger.error(
                (
                    "entity=%s "
                    "column=%s "
                    "null_count=%s"
                ),
                entity_name,
                column,
                null_count,
            )

            raise DataValidationError(
                (
                    f"Required field {column} "
                    f"contains {null_count} NULL values "
                    f"for entity={entity_name}"
                )
            )


def _validate_watermark(
    entity_name: str,
    df: pd.DataFrame,
    watermark_field: str,
) -> tuple[
    pd.DataFrame,
    str,
]:
    """
    Convert updated_at to a UTC datetime and
    calculate the maximum watermark.
    """

    validated_df = df.copy()

    # --------------------------------------------------------
    # Convert timestamp
    # --------------------------------------------------------

    validated_df[
        watermark_field
    ] = pd.to_datetime(
        validated_df[
            watermark_field
        ],
        utc=True,
        errors="coerce",
    )

    # --------------------------------------------------------
    # Detect invalid timestamps
    #
    # Invalid timestamps become NaT because
    # errors="coerce".
    # --------------------------------------------------------

    invalid_mask = (
        validated_df[
            watermark_field
        ]
        .isna()
    )

    invalid_count = int(
        invalid_mask.sum()
    )

    if invalid_count > 0:

        logger.error(
            (
                "entity=%s "
                "watermark_field=%s "
                "invalid_timestamp_count=%s"
            ),
            entity_name,
            watermark_field,
            invalid_count,
        )

        raise DataValidationError(
            (
                f"Invalid {watermark_field} values "
                f"found for entity={entity_name}. "
                f"Count={invalid_count}"
            )
        )

    # --------------------------------------------------------
    # Maximum watermark
    # --------------------------------------------------------

    max_timestamp = (
        validated_df[
            watermark_field
        ]
        .max()
    )

    max_watermark = (
        max_timestamp.isoformat()
    )

    return (
        validated_df,
        max_watermark,
    )


# ============================================================
# ENTITY-SPECIFIC VALIDATIONS
# ============================================================

def _validate_adverse_event(
    df: pd.DataFrame,
) -> None:

    # --------------------------------------------------------
    # Serious must be Y or N
    # --------------------------------------------------------

    if "serious" in df.columns:

        valid_values = {
            "Y",
            "N",
        }

        actual_values = set(
            df["serious"]
            .dropna()
            .astype(str)
            .str.upper()
            .unique()
        )

        invalid_values = (
            actual_values
            - valid_values
        )

        if invalid_values:

            raise DataValidationError(
                (
                    "Invalid adverse_event serious "
                    f"values: {sorted(invalid_values)}"
                )
            )

    # --------------------------------------------------------
    # Severity domain
    # --------------------------------------------------------

    if "severity" in df.columns:

        valid_values = {
            "MILD",
            "MODERATE",
            "SEVERE",
        }

        actual_values = set(
            df["severity"]
            .dropna()
            .astype(str)
            .str.upper()
            .unique()
        )

        invalid_values = (
            actual_values
            - valid_values
        )

        if invalid_values:

            raise DataValidationError(
                (
                    "Invalid adverse_event severity "
                    f"values: {sorted(invalid_values)}"
                )
            )


def _validate_subject(
    df: pd.DataFrame,
) -> None:

    if "age" not in df.columns:
        return

    # Numeric conversion for validation only
    ages = pd.to_numeric(
        df["age"],
        errors="coerce",
    )

    invalid_age = (
        ages.isna()
        |
        (ages < 0)
        |
        (ages > 120)
    )

    invalid_count = int(
        invalid_age.sum()
    )

    if invalid_count > 0:

        raise DataValidationError(
            (
                "Invalid subject age values found. "
                f"Count={invalid_count}"
            )
        )


def _run_entity_specific_validation(
    entity_name: str,
    df: pd.DataFrame,
) -> None:

    if entity_name == "adverse_event":

        _validate_adverse_event(
            df
        )

    elif entity_name == "subject":

        _validate_subject(
            df
        )


# ============================================================
# PUBLIC VALIDATION FUNCTION
# ============================================================

def validate_records(
    entity_name: str,
    records: list[dict],
) -> ValidationResult:
    """
    Convert parsed records into a Pandas DataFrame
    and perform data-quality validation.

    Empty incremental batches are valid.
    """

    logger.info(
        (
            "entity=%s "
            "validation_started "
            "input_record_count=%s"
        ),
        entity_name,
        len(records),
    )

    config = (
        _validate_entity_configuration(
            entity_name
        )
    )

    primary_key = (
        config["primary_key"]
    )

    watermark_field = (
        config["watermark_field"]
    )

    required_fields = (
        config["required_fields"]
    )

    # ========================================================
    # EMPTY INCREMENTAL BATCH
    # ========================================================

    if not records:

        logger.info(
            (
                "entity=%s "
                "no_incremental_records"
            ),
            entity_name,
        )

        return ValidationResult(
            entity_name=entity_name,
            dataframe=pd.DataFrame(),
            record_count=0,
            primary_key=primary_key,
            watermark_field=watermark_field,
            max_watermark=None,
        )

    # ========================================================
    # LIST[DICT] -> DATAFRAME
    # ========================================================

    df = pd.DataFrame(
        records
    )

    logger.info(
        (
            "entity=%s "
            "dataframe_created "
            "rows=%s "
            "columns=%s"
        ),
        entity_name,
        len(df),
        len(df.columns),
    )

    # ========================================================
    # REQUIRED COLUMNS
    # ========================================================

    _validate_required_columns(
        entity_name=entity_name,
        df=df,
        required_fields=required_fields,
    )

    # ========================================================
    # REQUIRED VALUES
    # ========================================================

    _validate_required_values(
        entity_name=entity_name,
        df=df,
        required_fields=required_fields,
    )

    # ========================================================
    # PRIMARY KEY
    # ========================================================

    _validate_primary_key(
        entity_name=entity_name,
        df=df,
        primary_key=primary_key,
    )

    # ========================================================
    # WATERMARK
    # ========================================================

    (
        df,
        max_watermark,
    ) = _validate_watermark(
        entity_name=entity_name,
        df=df,
        watermark_field=watermark_field,
    )

    # ========================================================
    # ENTITY-SPECIFIC RULES
    # ========================================================

    _run_entity_specific_validation(
        entity_name=entity_name,
        df=df,
    )

    # ========================================================
    # SUCCESS
    # ========================================================

    logger.info(
        (
            "entity=%s "
            "validation_completed "
            "record_count=%s "
            "max_watermark=%s"
        ),
        entity_name,
        len(df),
        max_watermark,
    )

    return ValidationResult(
        entity_name=entity_name,
        dataframe=df,
        record_count=len(df),
        primary_key=primary_key,
        watermark_field=watermark_field,
        max_watermark=max_watermark,
    )