from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

from common.schemas import get_bronze_schema


KAFKA_COLUMNS = [
    "event_id",
    "event_type",
    "event_timestamp",
    "entity",
]


def normalize(
    df: DataFrame,
    dataset: str,
    source: str,
) -> DataFrame:

    source = source.lower()

    # CSV files arrive without ingestion metadata.
    if source == "csv":

        df = (
            df
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_batch_id", lit("csv_batch"))
            .withColumn("_source_system", lit("csv"))
            .withColumn("_source_file", lit(f"{dataset}.csv"))
        )

    # Kafka landing contains transport metadata which is
    # not required beyond Landing.
    elif source == "kafka":

        for column in KAFKA_COLUMNS:
            if column in df.columns:
                df = df.drop(column)

    bronze_schema = get_bronze_schema(dataset)
    expected_columns = [field.name for field in bronze_schema.fields]

    missing_columns = [
        column
        for column in expected_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing columns for dataset '{dataset}': {missing_columns}"
        )

    return df.select(*expected_columns)