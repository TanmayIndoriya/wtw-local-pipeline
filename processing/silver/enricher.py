from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_date,
    datediff,
    floor,
    months_between,
    year,
)


def enrich(
    df: DataFrame,
    dataset: str,
) -> DataFrame:

    if dataset == "customers":

        return df.withColumn(
            "customer_age",
            floor(
                months_between(
                    current_date(),
                    "date_of_birth",
                ) / 12
            ),
        )

    if dataset == "policies":

        return df.withColumn(
            "policy_duration_days",
            datediff(
                "end_date",
                "start_date",
            ),
        )

    if dataset == "claims":

        return df.withColumn(
            "claim_year",
            year("claim_date"),
        )

    if dataset == "payments":

        return df.withColumn(
            "payment_year",
            year("payment_date"),
        )

    return df