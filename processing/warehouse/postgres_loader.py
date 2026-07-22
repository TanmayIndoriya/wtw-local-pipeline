from common.config import get_config
from common.logger import get_logger

from processing.common.reader import read_gold
from processing.common.reader import read_silver

logger = get_logger(__name__)


def write_table(df, table_name: str, mode: str = "overwrite") -> None:

    config = get_config().get_postgres()["postgres"]

    url = (
        f"jdbc:postgresql://"
        f"{config['host']}:{config['port']}/{config['database']}"
    )

    properties = {
        "user": config["username"],
        "password": config["password"],
        "driver": config["driver"],
    }

    logger.info(f"Writing {table_name} to PostgreSQL")

    (
        df.write
        .mode(mode)
        .jdbc(
            url=url,
            table=table_name,
            properties=properties,
        )
    )


def load_silver(spark):

    silver_tables = [
        "customers",
        "policies",
        "claims",
        "payments",
    ]

    for table in silver_tables:

        df = read_silver(spark, table)

        write_table(
            df,
            f"silver.{table}",
        )


def load_gold(spark):

    gold_tables = [
        "customer_summary",
        "policy_summary",
        "claim_summary",
        "payment_summary",
    ]

    for table in gold_tables:

        df = read_gold(spark, table)

        write_table(
            df,
            f"gold.{table}",
        )