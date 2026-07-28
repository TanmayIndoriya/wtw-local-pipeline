from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "policies": ["policy_id"],
    "claims": ["claim_id"],
    "payments": ["payment_id"],
}


def deduplicate(
    df: DataFrame,
    dataset: str,
) -> DataFrame:

    keys = PRIMARY_KEYS[dataset]

    # Customers: keep latest version for SCD
    if dataset == "customers":

        window = (
            Window
            .partitionBy(*keys)
            .orderBy(col("_ingestion_timestamp").desc())
        )

        return (
            df
            .withColumn(
                "_row_number",
                row_number().over(window),
            )
            .filter(col("_row_number") == 1)
            .drop("_row_number")
        )

    # Other datasets: simple deduplication
    return df.dropDuplicates(keys)