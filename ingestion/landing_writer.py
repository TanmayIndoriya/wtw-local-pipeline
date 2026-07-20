from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame

from common.constants import LANDING, PARQUET
from common.filesystem import get_partition_path
from common.logger import get_logger

logger = get_logger(__name__)


class LandingWriter:
    """
    Writes raw data into the Landing layer.
    """

    def write(
        self,
        df: DataFrame,
        source: str,
        dataset: str,
        mode: str = "append",
    ) -> Path:
        """
        Writes a DataFrame to the Landing layer.

        Directory layout:

        storage/
            landing/
                mysql/
                    customers/
                        ingestion_date=2026-07-20/
                csv/
                    customers/
                        ingestion_date=2026-07-20/
                kafka/
                    claims/
                        ingestion_date=2026-07-20/
        """

        partition = datetime.now().strftime("%Y-%m-%d")

        output_path = get_partition_path(
            LANDING,
            f"{source}/{dataset}",
            f"ingestion_date={partition}",
        )

        logger.info(
            "Writing dataset '%s' from source '%s' to '%s'",
            dataset,
            source,
            output_path,
        )

        (
            df.write
            .mode(mode)
            .format(PARQUET)
            .save(str(output_path))
        )

        logger.info(
            "Successfully wrote %d records.",
            df.count(),
        )

        return output_path


landing_writer = LandingWriter()