from pyspark.sql import SparkSession # type: ignore
from typing import Optional

from config import get_config


_spark: Optional[SparkSession] = None


def _create_spark_session() -> SparkSession:

    spark_config = get_config().get_spark()["spark"]

    builder = (
        SparkSession.builder
        .appName(spark_config["app_name"])
        .master(spark_config["master"])
        .config(
            "spark.sql.shuffle.partitions",
            spark_config["shuffle_partitions"],
        )
        .config(
            "spark.sql.parquet.compression.codec",
            spark_config["parquet_compression"],
        )
        .config(
            "spark.sql.adaptive.enabled",
            spark_config["adaptive_query_execution"],
        )
    )

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel(
        spark_config["log_level"]
    )

    return spark


def get_spark() -> SparkSession:
    
    global _spark

    if _spark is None:
        _spark = _create_spark_session()

    return _spark


def stop_spark() -> None:
    
    global _spark

    if _spark is not None:
        _spark.stop()
        _spark = None


def spark_is_running() -> bool:
    return _spark is not None