from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max,
    month,
    sum,
    when,
    year,
)

from processing.common.reader import read_silver


def build(spark) -> DataFrame:

    claims = read_silver(
        spark,
        "claims",
    )

    return (
        claims
        .withColumn(
            "claim_year",
            year("claim_date"),
        )
        .withColumn(
            "claim_month",
            month("claim_date"),
        )
        .groupBy(
            "claim_year",
            "claim_month",
        )
        .agg(

            count("claim_id").alias(
                "total_claims",
            ),

            count(
                when(
                    col("claim_status") == "APPROVED",
                    True,
                )
            ).alias(
                "approved_claims",
            ),

            count(
                when(
                    col("claim_status") == "REJECTED",
                    True,
                )
            ).alias(
                "rejected_claims",
            ),

            sum("claim_amount").alias(
                "total_claim_amount",
            ),

            avg("claim_amount").alias(
                "average_claim_amount",
            ),

            max("claim_amount").alias(
                "highest_claim_amount",
            ),
        )
        .orderBy(
            "claim_year",
            "claim_month",
        )
    )