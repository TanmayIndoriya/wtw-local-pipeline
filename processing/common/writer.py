from pyspark.sql import DataFrame

from common.filesystem import get_dataset_path


def write_bronze(
    df: DataFrame,
    dataset: str,
    mode: str = "overwrite",
) -> None:
    path = get_dataset_path("bronze", dataset)

    (
        df.write
        .mode(mode)
        .parquet(str(path))
    )


def write_silver(
    df: DataFrame,
    dataset: str,
    mode: str = "overwrite",
) -> None:
    path = get_dataset_path("silver", dataset)

    (
        df.write
        .mode(mode)
        .parquet(str(path))
    )


def write_gold(
    df: DataFrame,
    dataset: str,
    mode: str = "overwrite",
) -> None:
    path = get_dataset_path("gold", dataset)

    (
        df.write
        .mode(mode)
        .parquet(str(path))
    )