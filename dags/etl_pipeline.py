import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ingestion.ingest_customer_addresses import run as ingest_customer_addresses  # noqa: E402
from transform.clean_customer_addresses import run as clean_customer_addresses  # noqa: E402
from transform.build_sales_report import run as build_sales_report  # noqa: E402
from transform.build_service_report import run as build_service_report  # noqa: E402


default_args = {
    "owner": "data-eng",
    "retries": 1,
}


with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["maju-jaya"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_customer_addresses",
        python_callable=ingest_customer_addresses,
    )

    clean_task = PythonOperator(
        task_id="clean_customer_addresses",
        python_callable=clean_customer_addresses,
    )

    sales_report_task = PythonOperator(
        task_id="build_sales_report",
        python_callable=build_sales_report,
    )

    service_report_task = PythonOperator(
        task_id="build_service_report",
        python_callable=build_service_report,
    )

    ingest_task >> clean_task >> [sales_report_task, service_report_task]
