"""
Generic ingestion validations.
"""

from pyspark.sql import DataFrame


def validate_not_empty(df: DataFrame) -> None:
    """
    Ensures dataframe is not empty.
    """

    if df.rdd.isEmpty():
        raise ValueError("DataFrame is empty.")


def validate_schema(
    df: DataFrame,
    expected_schema,
) -> None:
    """
    Validates dataframe schema.
    """

    actual = df.schema

    if actual != expected_schema:
        raise ValueError(
            f"Schema mismatch.\nExpected:\n{expected_schema}\n\nActual:\n{actual}"
        )