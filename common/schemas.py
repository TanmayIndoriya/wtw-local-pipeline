from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
    TimestampType,
    LongType,
)


def customer_schema() -> StructType:

    return StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("registration_date", DateType(), True),
    ])


def policy_schema() -> StructType:

    return StructType([
        StructField("policy_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("policy_type", StringType(), True),
        StructField("premium_amount", LongType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("status", StringType(), True),
    ])


def claim_schema() -> StructType:

    return StructType([
        StructField("claim_id", StringType(), True),
        StructField("policy_id", StringType(), True),
        StructField("claim_date", DateType(), True),
        StructField("claim_amount", LongType(), True),
        StructField("claim_status", StringType(), True),
    ])


def payment_schema() -> StructType:

    return StructType([
        StructField("payment_id", StringType(), True),
        StructField("policy_id", StringType(), True),
        StructField("payment_date", DateType(), True),
        StructField("payment_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
    ])


# -------------------------------
# Business Schemas
# -------------------------------

SCHEMAS = {
    "customers": customer_schema(),
    "policies": policy_schema(),
    "claims": claim_schema(),
    "payments": payment_schema(),
}


# -------------------------------
# Processing Metadata
# -------------------------------

METADATA_FIELDS = [
    StructField("_ingestion_timestamp", TimestampType(), False),
    StructField("_batch_id", StringType(), False),
    StructField("_source_system", StringType(), False),
    StructField("_source_file", StringType(), True),
]


# -------------------------------
# Schema APIs
# -------------------------------

def get_schema(dataset: str) -> StructType:

    try:
        return SCHEMAS[dataset.lower()]
    except KeyError:
        raise ValueError(f"Unsupported dataset: {dataset}")


def get_bronze_schema(dataset: str) -> StructType:

    business_schema = get_schema(dataset)

    return StructType(
        list(business_schema.fields) + METADATA_FIELDS
    )


def get_silver_schema(dataset: str) -> StructType:

    # Currently identical to Bronze.
    # Can diverge later if Silver adds derived columns.
    return get_bronze_schema(dataset)


def get_gold_schema(dataset: str) -> StructType:

    raise NotImplementedError(
        "Gold schemas are dataset-specific and will be implemented later."
    )