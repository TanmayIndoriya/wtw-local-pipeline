from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)


def customer_schema() -> StructType:

    return StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("date_of_birth", DateType(), True),
        StructField("gender", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])


def policy_schema() -> StructType:

    return StructType([
        StructField("policy_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("policy_type", StringType(), False),
        StructField("coverage_amount", DoubleType(), False),
        StructField("premium_amount", DoubleType(), False),
        StructField("deductible", DoubleType(), True),
        StructField("start_date", DateType(), False),
        StructField("end_date", DateType(), False),
        StructField("policy_status", StringType(), False),
        StructField("agent_id", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])


def claim_schema() -> StructType:

    return StructType([
        StructField("claim_id", StringType(), False),
        StructField("policy_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("claim_date", DateType(), False),
        StructField("incident_date", DateType(), True),
        StructField("claim_amount", DoubleType(), False),
        StructField("approved_amount", DoubleType(), True),
        StructField("claim_status", StringType(), False),
        StructField("claim_reason", StringType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
    ])


def payment_schema() -> StructType:

    return StructType([
        StructField("payment_id", StringType(), False),
        StructField("policy_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("payment_date", DateType(), False),
        StructField("payment_amount", DoubleType(), False),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), False),
        StructField("transaction_reference", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ])


SCHEMAS = {
    "customers": customer_schema(),
    "policies": policy_schema(),
    "claims": claim_schema(),
    "payments": payment_schema(),
}


def get_schema(dataset: str) -> StructType:

    try:
        return SCHEMAS[dataset.lower()]
    except KeyError:
        raise ValueError(f"Unsupported dataset: {dataset}")