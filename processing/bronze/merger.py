from functools import reduce

from pyspark.sql import DataFrame

from functools import reduce
from pyspark.sql import DataFrame


def merge(
    dataframes: list[DataFrame],
) -> DataFrame:
    """
    Merge normalized Bronze dataframes.
    """

    if not dataframes:
        raise ValueError("No dataframes supplied for merge.")

    return reduce(
        lambda left, right: left.unionByName(
            right,
            allowMissingColumns=False,
        ),
        dataframes,
    )