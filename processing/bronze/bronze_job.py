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

def run(dataset: str) -> None:

    spark = get_spark()

    try:
        dataframes = []

        for source in SOURCES:

            df = read_landing(
                spark=spark,
                source=source,
                dataset=dataset,
            )

            df = normalize(
                df=df,
                dataset=dataset,
                source=source
            )

            dataframes.append(df)

        bronze_df = merge(dataframes)

        write_bronze(
            df=bronze_df,
            dataset=dataset,
        )

    except Exception as e:
        logger.warn(e)