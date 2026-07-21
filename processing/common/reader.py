from pyspark.sql import DataFrame, SparkSession

from common.filesystem import get_dataset_path
from common.schemas import get_schema

def read_landing(
    spark: SparkSession,
    source: str,
    dataset: str,
) -> DataFrame:

    path = get_dataset_path(
        "landing",
        f"{source}/{dataset}",
    )

    if source.lower() == "csv":

        csv_path = path.parent / f"{dataset}.csv"

        return (
            spark.read
            .schema(get_schema(dataset))
            .option("header", True)
            .csv(str(csv_path))
        )

    return spark.read.parquet(str(path))


def read_bronze(
    spark: SparkSession,
    dataset: str,
) -> DataFrame:
    path = get_dataset_path("bronze", dataset)
    return spark.read.parquet(str(path))


def read_silver(
    spark: SparkSession,
    dataset: str,
) -> DataFrame:
    path = get_dataset_path("silver", dataset)
    return spark.read.parquet(str(path))


def read_gold(
    spark: SparkSession,
    dataset: str,
) -> DataFrame:
    path = get_dataset_path("gold", dataset)
    return spark.read.parquet(str(path))