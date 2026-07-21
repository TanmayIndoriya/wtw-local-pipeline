from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from common.logger import get_logger

logger = get_logger(__name__)


def validate_not_empty(df: DataFrame):

    if df.isEmpty():
        raise ValueError("DataFrame is empty.")


def validate_schema(
    df: DataFrame,
    expected_schema: StructType,
):

    actual_fields = {
        field.name: field.dataType
        for field in df.schema.fields
    }

    missing = []
    mismatched = []

    for field in expected_schema.fields:

        if field.name not in actual_fields:
            missing.append(field.name)
            continue

        if actual_fields[field.name] != field.dataType:
            mismatched.append(
                (
                    field.name,
                    field.dataType,
                    actual_fields[field.name],
                )
            )

    if missing:

        raise ValueError(
            f"Missing columns: {missing}"
        )

    if mismatched:

        errors = "\n".join(
            f"{name}: expected {expected}, got {actual}"
            for name, expected, actual in mismatched
        )

        raise ValueError(
            f"Schema type mismatch:\n{errors}"
        )

    logger.info("Schema validation passed.")