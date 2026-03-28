
import os
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException



DATA_DIR = "data/raw/customer_addresses"
TABLE_NAME = "customer_addresses_raw"


@dag(
    start_date=datetime(2026, 3, 2),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ingestion"],
)
def customer_address_ingest_1d():

    start = EmptyOperator(task_id="start")

    @task(execution_timeout=timedelta(hours=2))
    def check_data(data_interval_start=None):
        # bizdate = H-1
        biz_date = data_interval_start.subtract(days=1).strftime("%Y%m%d")

        file_path = Path(DATA_DIR) / f"customer_addresses_{biz_date}.csv"

        if not file_path.exists():
            raise AirflowFailException(f"Data does not exist for {biz_date}")

        print(f"Found file: {file_path}")
        return str(file_path)

   
    @task(execution_timeout=timedelta(hours=2))
    def ingest_data(file_check: str):
        print(f"Processing file: {file_check}")

        load_dotenv()

        engine = create_engine(
            f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )

        df = pd.read_csv(file_check)

        if df.empty:
            raise ValueError("CSV is empty")

        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists="append",
            index=False
        )

        print(f"Inserted {len(df)} rows into {TABLE_NAME}")

    end = EmptyOperator(task_id="end")

   
    file_check = check_data()
    ingest = ingest_data(file_check)

    start >> file_check >> ingest >> end


dag = customer_address_ingest_1d()