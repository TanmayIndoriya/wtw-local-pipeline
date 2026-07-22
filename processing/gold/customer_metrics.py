from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat_ws,
    count,
    lit,
    sum,
    when,
)

from processing.common.reader import read_silver


def build(spark) -> DataFrame:

    customers = read_silver(spark, "customers")

    policies = read_silver(spark, "policies")

    claims = read_silver(spark, "claims")

    payments = read_silver(spark, "payments")

    # -------------------------------------------------
    # Policy metrics per customer
    # -------------------------------------------------

    policy_metrics = (
        policies
        .groupBy("customer_id")
        .agg(
            count("policy_id").alias("total_policies"),
            count(
                when(
                    col("status") == "ACTIVE",
                    True,
                )
            ).alias("active_policies"),
        )
    )

    # -------------------------------------------------
    # Claim metrics per customer
    # -------------------------------------------------

    claim_metrics = (
        policies
        .join(
            claims,
            "policy_id",
            "left",
        )
        .groupBy("customer_id")
        .agg(
            count("claim_id").alias("total_claims"),

            count(
                when(
                    col("claim_status") == "APPROVED",
                    True,
                )
            ).alias("approved_claims"),

            count(
                when(
                    col("claim_status") == "REJECTED",
                    True,
                )
            ).alias("rejected_claims"),

            coalesce(
                sum("claim_amount"),
                lit(0),
            ).alias("total_claim_amount"),
        )
    )

    # -------------------------------------------------
    # Payment metrics per customer
    # -------------------------------------------------

    payment_metrics = (
        policies
        .join(
            payments,
            "policy_id",
            "left",
        )
        .groupBy("customer_id")
        .agg(
            count("payment_id").alias("total_payments"),

            coalesce(
                sum("payment_amount"),
                lit(0.0),
            ).alias("total_payment_amount"),

            coalesce(
                avg("payment_amount"),
                lit(0.0),
            ).alias("average_payment"),
        )
    )

    # -------------------------------------------------
    # Final Gold Dataset
    # -------------------------------------------------

    return (
        customers
        .join(
            policy_metrics,
            "customer_id",
            "left",
        )
        .join(
            claim_metrics,
            "customer_id",
            "left",
        )
        .join(
            payment_metrics,
            "customer_id",
            "left",
        )
        .select(
            "customer_id",

            concat_ws(
                " ",
                col("first_name"),
                col("last_name"),
            ).alias("customer_name"),

            "city",
            "state",
            "registration_date",

            coalesce(
                col("total_policies"),
                lit(0),
            ).alias("total_policies"),

            coalesce(
                col("active_policies"),
                lit(0),
            ).alias("active_policies"),

            coalesce(
                col("total_claims"),
                lit(0),
            ).alias("total_claims"),

            coalesce(
                col("approved_claims"),
                lit(0),
            ).alias("approved_claims"),

            coalesce(
                col("rejected_claims"),
                lit(0),
            ).alias("rejected_claims"),

            coalesce(
                col("total_claim_amount"),
                lit(0),
            ).alias("total_claim_amount"),

            coalesce(
                col("total_payments"),
                lit(0),
            ).alias("total_payments"),

            coalesce(
                col("total_payment_amount"),
                lit(0.0),
            ).alias("total_payment_amount"),

            coalesce(
                col("average_payment"),
                lit(0.0),
            ).alias("average_payment"),
        )
        .withColumn(
            "customer_lifetime_value",
            col("total_payment_amount"),
        )
    )