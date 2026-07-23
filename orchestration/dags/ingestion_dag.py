from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore

from orchestration.tasks.ingestion_tasks import (
    ingest_mysql,
    ingest_kafka,
)
from orchestration.utils.default_args import default_args


with DAG(
    dag_id="ingestion_pipeline",
    description="Ingest data from MySQL and Kafka",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ingestion"],
) as dag:

    mysql_ingestion = PythonOperator(
        task_id="mysql_ingestion",
        python_callable=ingest_mysql,
    )

    kafka_ingestion = PythonOperator(
        task_id="kafka_ingestion",
        python_callable=ingest_kafka,
    )

    # Run both independently
    [mysql_ingestion, kafka_ingestion]