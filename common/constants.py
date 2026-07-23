
# ==========================================================
# Storage Layers
# ==========================================================

LANDING = "landing"
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
CHECKPOINTS = "checkpoints"
QUARANTINE = "quarantine"

STORAGE_LAYERS = (
    LANDING,
    BRONZE,
    SILVER,
    GOLD,
    CHECKPOINTS,
    QUARANTINE,
)

# ==========================================================
# Dataset Names
# ==========================================================

CUSTOMERS = "customers"
POLICIES = "policies"
CLAIMS = "claims"
PAYMENTS = "payments"

DATASETS = (
    CUSTOMERS,
    POLICIES,
    CLAIMS,
    PAYMENTS,
)

# ==========================================================
# File Formats
# ==========================================================

CSV = "csv"
JSON = "json"
PARQUET = "parquet"
DELTA = "delta"

# ==========================================================
# Write Modes
# ==========================================================

OVERWRITE = "overwrite"
APPEND = "append"
IGNORE = "ignore"
ERROR_IF_EXISTS = "error"

# ==========================================================
# Common Metadata Columns
# ==========================================================

INGESTION_TIMESTAMP = "_ingestion_timestamp"
PROCESSING_TIMESTAMP = "_processing_timestamp"
SOURCE_SYSTEM = "_source_system"
SOURCE_FILE = "_source_file"
RECORD_HASH = "_record_hash"
LOAD_DATE = "_load_date"
BATCH_ID = "_batch_id"

METADATA_COLUMNS = (
    INGESTION_TIMESTAMP,
    PROCESSING_TIMESTAMP,
    SOURCE_SYSTEM,
    SOURCE_FILE,
    RECORD_HASH,
    LOAD_DATE,
    BATCH_ID,
)

# ==========================================================
# Record Status
# ==========================================================

ACTIVE = "ACTIVE"
INACTIVE = "INACTIVE"

VALID = "VALID"
INVALID = "INVALID"

SUCCESS = "SUCCESS"
FAILED = "FAILED"

# ==========================================================
# Source Systems
# ==========================================================

MYSQL = "mysql"
CSV_SOURCE = "csv"
KAFKA = "kafka"

# ==========================================================
# Partition Columns
# ==========================================================

YEAR = "year"
MONTH = "month"
DAY = "day"

PARTITION_COLUMNS = (
    YEAR,
    MONTH,
    DAY,
)

# ==========================================================
# Date Formats
# ==========================================================

DATE_FORMAT = "yyyy-MM-dd"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# ==========================================================
# Common Boolean Values
# ==========================================================

YES = "Y"
NO = "N"

TRUE = "true"
FALSE = "false"

# ==========================================================
# Null Representations
# ==========================================================

NULL = "NULL"
EMPTY_STRING = ""

# ==========================================================
# Default Spark Config Values
# (Used only as fallbacks if config is unavailable)
# ==========================================================

DEFAULT_SHUFFLE_PARTITIONS = 8
DEFAULT_PARQUET_COMPRESSION = "snappy"

# ==========================================================
# Hashing
# ==========================================================

HASH_ALGORITHM = "sha256"

# ==========================================================
# Logging
# ==========================================================

LOGGER_NAME = "data-platform"

# ==========================================================
# Datasets
# ==========================================================

DATASETS = [
    "customers",
    "policies",
    "claims",
    "payments",
]

MYSQL_TABLES = {
    "customers": "customers",
    "policies": "policies",
    "claims": "claims",
    "payments": "payments",
}

KAFKA_TOPICS = [
    "customers",
    "policies",
    "claims",
    "payments",
]
