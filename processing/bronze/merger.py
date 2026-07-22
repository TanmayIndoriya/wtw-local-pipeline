from pyspark.sql import DataFrame

def merge(
    dfs: list[DataFrame],
) -> DataFrame:

    if not dfs:
        raise ValueError("No dataframes supplied for merge.")

    merged = dfs[0]

    for df in dfs[1:]:
        merged = merged.unionByName(
            df,
            allowMissingColumns=True,
        )

    return merged