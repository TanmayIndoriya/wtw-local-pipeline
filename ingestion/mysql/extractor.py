from pyspark.sql import DataFrame

from common.config import get_config
from common.logger import get_logger
from common.spark import get_spark

logger = get_logger(__name__)


class MySQLExtractor:

    def __init__(self):
        self.spark = get_spark()
        self.config = get_config().get_mysql()["mysql"]

    def extract(
        self,
        table: str,
    ) -> DataFrame:
        """
        Reads a MySQL table into a Spark DataFrame.
        """

        logger.info("Reading MySQL table: %s", table)

        jdbc_url = (
            f"jdbc:mysql://{self.config['host']}:{self.config['port']}/"
            f"{self.config['database']}"
        )

        df = (
            self.spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("driver", self.config["driver"])
            .option("dbtable", table)
            .option("user", self.config["username"])
            .option("password", self.config["password"])
            .load()
        )

        logger.info("Successfully read %d records", df.count())

        return df