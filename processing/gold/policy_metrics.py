from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    sum,
    when,
    coalesce,
    lit,
)

from processing.common.reader import read_silver


def build(spark) -> DataFrame:

    policies = read_silver(
        spark,
        "policies",
    ).alias("p")

    claims = read_silver(
        spark,
        "claims",
    ).alias("c")

    payments = read_silver(
        spark,
        "payments",
    ).alias("pm")

    claim_metrics = (
        claims
        .groupBy("policy_id")
        .agg(
            count("claim_id").alias("total_claims"),
            coalesce(
                sum("claim_amount"),
                lit(0),
            ).alias("total_claim_amount"),
        )
    )

    payment_metrics = (
        payments
        .groupBy("policy_id")
        .agg(
            count("payment_id").alias("total_payments"),
            coalesce(
                sum("payment_amount"),
                lit(0.0),
            ).alias("total_payment_amount"),
        )
    )

    return (
        policies
        .join(
            claim_metrics,
            "policy_id",
            "left",
        )
        .join(
            payment_metrics,
            "policy_id",
            "left",
        )
        .groupBy("policy_type")
        .agg(

            count("policy_id").alias(
                "total_policies",
            ),

            count(
                when(
                    col("status") == "ACTIVE",
                    True,
                )
            ).alias(
                "active_policies",
            ),

            count(
                when(
                    col("status") == "EXPIRED",
                    True,
                )
            ).alias(
                "expired_policies",
            ),

            avg("premium_amount").alias(
                "average_premium",
            ),

            sum("premium_amount").alias(
                "total_premium",
            ),

            coalesce(
                sum("total_claim_amount"),
                lit(0),
            ).alias(
                "total_claim_amount",
            ),

            coalesce(
                sum("total_payment_amount"),
                lit(0.0),
            ).alias(
                "total_payment_amount",
            ),
        )
        .withColumn(
            "claim_ratio",
            when(
                col("total_premium") == 0,
                lit(0),
            ).otherwise(
                col("total_claim_amount")
                / col("total_premium")
            ),
        )
    )