"""
Adds ingestion metadata columns to DataFrames.
"""

from datetime import datetime
import uuid

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit


def add_metadata(
    df: DataFrame,
    source: str,
    source_file: str | None = None,
) -> DataFrame:
    """
    Adds ingestion metadata columns.

    Columns added:
    - _ingestion_timestamp
    - _batch_id
    - _source_system
    - _source_file
    """

    batch_id = str(uuid.uuid4())

    return (
        df.withColumn(
            "_ingestion_timestamp",
            current_timestamp(),
        )
        .withColumn(
            "_batch_id",
            lit(batch_id),
        )
        .withColumn(
            "_source_system",
            lit(source),
        )
        .withColumn(
            "_source_file",
            lit(source_file),
        )
    )