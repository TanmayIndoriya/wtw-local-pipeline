from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    TimestampType,
)

TRACKED_COLUMNS = [
    "first_name",
    "last_name",
    "city",
    "state",
]

BUSINESS_KEY = "customer_id"


def apply_scd(
    incoming_df: DataFrame,
    current_df: DataFrame | None,
) -> DataFrame:

    now = current_timestamp()

    if current_df is None:

        return (
            incoming_df
            .withColumn("valid_from", now)
            .withColumn("valid_to", lit(None).cast(TimestampType()))
            .withColumn("current_flag", lit(True))
            .withColumn("version", lit(1))
        )

    current = current_df.filter(col("current_flag"))

    history = current_df.filter(~col("current_flag"))

    incoming = incoming_df.alias("incoming")
    existing = current.alias("existing")

    joined = incoming.join(
        existing,
        BUSINESS_KEY,
        "left",
    )

    
    change_condition = None

    for c in TRACKED_COLUMNS:

        cond = col(f"incoming.{c}") != col(f"existing.{c}")

        if change_condition is None:
            change_condition = cond
        else:
            change_condition |= cond

    new_customers = (
        joined
        .filter(col("existing.customer_id").isNull())
        .select("incoming.*")
        .withColumn("valid_from", now)
        .withColumn("valid_to", lit(None).cast(TimestampType()))
        .withColumn("current_flag", lit(True))
        .withColumn("version", lit(1))
    )


    changed = joined.filter(change_condition)

    expired = (
        changed
        .select("existing.*")
        .withColumn("valid_to", now)
        .withColumn("current_flag", lit(False))
    )

    new_versions = (
        changed
        .select("incoming.*", col("existing.version"))
        .withColumn("valid_from", now)
        .withColumn("valid_to", lit(None).cast(TimestampType()))
        .withColumn("current_flag", lit(True))
        .withColumn("version", col("version") + 1)
    )

    unchanged = (
        joined
        .filter(~change_condition & col("existing.customer_id").isNotNull())
        .select("existing.*")
    )

    type1 = (
        unchanged
        .join(
            incoming_df.select(
                BUSINESS_KEY,
                "email",
                "phone",
            ),
            BUSINESS_KEY,
        )
        .drop("email", "phone")
        .withColumnRenamed("email", "tmp")
    )

    type1 = (
        unchanged.alias("e")
        .join(
            incoming_df.select(
                BUSINESS_KEY,
                "email",
                "phone",
            ).alias("i"),
            BUSINESS_KEY,
        )
        .select(
            col("e.customer_id"),
            col("e.first_name"),
            col("e.last_name"),
            col("e.gender"),
            col("e.date_of_birth"),
            col("i.email").alias("email"),
            col("i.phone").alias("phone"),
            col("e.city"),
            col("e.state"),
            col("e.registration_date"),
            col("e._ingestion_timestamp"),
            col("e.customer_age"),
            col("e._batch_id"),
            col("e._source_system"),
            col("e._source_file"),
            col("e.valid_from"),
            col("e.valid_to"),
            col("e.current_flag"),
            col("e.version"),
        )
    )

    return (
        history
        .unionByName(expired)
        .unionByName(type1)
        .unionByName(new_versions)
        .unionByName(new_customers)
    )