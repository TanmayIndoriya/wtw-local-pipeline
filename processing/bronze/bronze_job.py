from common.spark import get_spark

from processing.common.reader import read_landing
from processing.common.writer import write_bronze

from processing.bronze.normalizer import normalize
from processing.bronze.merger import merge

from common.logger import get_logger


SOURCES = [
    "mysql",
    "csv",
    "kafka",
]

logger = get_logger(__name__)


def run(spark, dataset: str) -> None:

    dataframes = []

    for source in SOURCES:
        try:
            df = read_landing(
                spark=spark,
                source=source,
                dataset=dataset,
            )

            df = normalize(
                df=df,
                dataset=dataset,
                source=source,
            )

            dataframes.append(df)
            logger.info(f"Loaded {source} data for {dataset}")

        except Exception as e:
            logger.warning(f"Skipping {source} for {dataset}: {e}")

    if not dataframes:
        raise RuntimeError(f"No landing data found for dataset '{dataset}'.")

    bronze_df = merge(dataframes)

    write_bronze(
        df=bronze_df,
        dataset=dataset,
    )

    logger.info(f"Bronze dataset '{dataset}' written successfully.")