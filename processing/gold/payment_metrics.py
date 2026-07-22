from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    count,
    month,
    sum,
    year,
)

from processing.common.reader import read_silver


def build(spark) -> DataFrame:

    payments = read_silver(
        spark,
        "payments",
    )

    return (
        payments
        .withColumn(
            "payment_year",
            year("payment_date"),
        )
        .withColumn(
            "payment_month",
            month("payment_date"),
        )
        .groupBy(
            "payment_year",
            "payment_month",
            "payment_method",
        )
        .agg(

            count("payment_id").alias(
                "total_payments",
            ),

            sum("payment_amount").alias(
                "total_payment_amount",
            ),

            avg("payment_amount").alias(
                "average_payment_amount",
            ),
        )
        .orderBy(
            "payment_year",
            "payment_month",
            "payment_method",
        )
    )