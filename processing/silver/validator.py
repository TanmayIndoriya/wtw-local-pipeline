from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def validate(
    df: DataFrame,
    dataset: str,
) -> tuple[DataFrame, DataFrame]:
    """
    Split dataframe into valid and invalid records.
    """

    if dataset == "customers":

        valid = df.filter(
            col("customer_id").isNotNull()
        )

    elif dataset == "policies":

        valid = df.filter(
            (col("policy_id").isNotNull()) &
            (col("premium_amount") > 0)
        )

    elif dataset == "claims":

        valid = df.filter(
            (col("claim_id").isNotNull()) &
            (col("claim_amount") >= 0)
        )

    elif dataset == "payments":

        valid = df.filter(
            (col("payment_id").isNotNull()) &
            (col("payment_amount") > 0)
        )

    else:
        raise ValueError(f"Unsupported dataset: {dataset}")

    invalid = df.subtract(valid)

    return valid, invalid