from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore

from orchestration.tasks.processing_tasks import (
    run_bronze,
    run_silver,
    run_gold,
    run_warehouse,
)
from orchestration.utils.default_args import default_args

from common.constants import DATASETS

with DAG(
    dag_id="processing_pipeline",
    description="Bronze -> Silver -> Gold -> Warehouse",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["processing"],
) as dag:

    bronze_tasks = {}
    silver_tasks = {}

    for dataset in DATASETS:

        bronze_tasks[dataset] = PythonOperator(
            task_id=f"bronze_{dataset}",
            python_callable=run_bronze,
            op_kwargs={"dataset": dataset},
        )

        silver_tasks[dataset] = PythonOperator(
            task_id=f"silver_{dataset}",
            python_callable=run_silver,
            op_kwargs={"dataset": dataset},
        )

        bronze_tasks[dataset] >> silver_tasks[dataset]

    gold = PythonOperator(
        task_id="gold",
        python_callable=run_gold,
    )

    warehouse = PythonOperator(
        task_id="warehouse",
        python_callable=run_warehouse,
    )

    for task in silver_tasks.values():
        task >> gold

    gold >> warehouse