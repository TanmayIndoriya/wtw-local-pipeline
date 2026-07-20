from pathlib import Path

from pyspark.sql import DataFrame

from common.logger import get_logger
from common.spark import get_spark

logger = get_logger(__name__)


class CSVExtractor:

    def __init__(self):
        self.spark = get_spark()

    def extract(
        self,
        file_path: str | Path,
        schema=None,
        header: bool = True,
    ) -> DataFrame:
        """
        Reads a CSV file into a Spark DataFrame.
        """

        logger.info("Reading CSV file: %s", file_path)

        reader = (
            self.spark.read
            .option("header", header)
        )

        if schema is not None:
            reader = reader.schema(schema)

        df = reader.csv(str(file_path))

        logger.info("Successfully read %d records", df.count())

        return df