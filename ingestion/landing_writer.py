from datetime import datetime

from pyspark.sql import DataFrame

from common.constants import LANDING, PARQUET
from common.filesystem import get_partition_path
from common.logger import get_logger

logger = get_logger(__name__)


class LandingWriter:

    def write(
        self,
        df: DataFrame,
        source: str,
        dataset: str,
    ) -> str:
        """
        Writes a dataframe to the landing layer.

        storage/
            landing/
                mysql/
                    customers/
                        ingestion_date=2026-07-20/
                kafka/
                    customers/
                        ingestion_date=2026-07-20/
        """

        partition = datetime.now().strftime("%Y-%m-%d")

        output_path = get_partition_path(
            LANDING,
            f"{source}/{dataset}",
            f"ingestion_date={partition}",
        )

        logger.info(
            "Writing dataset '%s' from '%s' to %s",
            dataset,
            source,
            output_path,
        )

        (
            df.write
            .mode("append")
            .format(PARQUET)
            .save(str(output_path))
        )

        logger.info("Landing write completed.")

        return output_path


landing_writer = LandingWriter()