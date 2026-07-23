from processing.bronze import bronze_job
from processing.silver import silver_job
from processing.gold import gold_job
from processing.warehouse import warehouse_job
from common.spark import get_spark

spark = get_spark()

def run_bronze(dataset: str):
    bronze_job.run(spark, dataset)


def run_silver(dataset: str):
    silver_job.run(spark, dataset)


def run_gold():
    gold_job.run(spark)


def run_warehouse():
    warehouse_job.run(spark)