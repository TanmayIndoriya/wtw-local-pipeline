from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper, when
from pyspark.sql.types import StringType


def clean(df: DataFrame) -> DataFrame:
    """
    Standardize dataframe values.

    - Trim whitespace
    - Convert empty strings to NULL
    - Uppercase string values
    """

    for field in df.schema.fields:

        if isinstance(field.dataType, StringType):

            df = df.withColumn(
                field.name,
                when(
                    trim(col(field.name)) == "",
                    None,
                ).otherwise(
                    upper(trim(col(field.name)))
                ),
            )

    return df