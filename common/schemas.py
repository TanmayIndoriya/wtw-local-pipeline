from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
    LongType
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