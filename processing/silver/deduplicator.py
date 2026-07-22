from pyspark.sql import DataFrame


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
    """
    Remove duplicate records.
    """

    return df.dropDuplicates(
        PRIMARY_KEYS[dataset]
    )