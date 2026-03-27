import argparse
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


FILENAME_RE = re.compile(r"customer_addresses_(\d{8})\.csv$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest daily customer_addresses_YYYYMMDD.csv into MySQL."
    )
    parser.add_argument(
        "--input-dir",
        default="data/raw/customer_addresses",
        help="Directory containing daily CSV files.",
    )
    parser.add_argument(
        "--fallback-dir",
        default="data/raw",
        help="Fallback directory if no files in input-dir.",
    )
    parser.add_argument(
        "--table",
        default="customer_addresses_raw",
        help="Target MySQL table name.",
    )
    return parser.parse_args()


def build_db_url() -> str:
    load_dotenv()
    db_url = os.getenv("DB_URL")
    if db_url:
        return db_url
    host = os.getenv("DB_HOST", "mysql")
    port = os.getenv("DB_PORT", "3306")
    user = os.getenv("DB_USER", "data_user")
    password = os.getenv("DB_PASSWORD", "data_pass")
    database = os.getenv("DB_NAME", "warehouse")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


def find_latest_file(input_dir: Path, fallback_dir: Path) -> Path:
    candidates = list(input_dir.glob("customer_addresses_*.csv"))
    if not candidates:
        candidates = list(fallback_dir.glob("customer_addresses_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No customer_addresses_YYYYMMDD.csv found in {input_dir} or {fallback_dir}."
        )

    def key_fn(p: Path) -> datetime:
        match = FILENAME_RE.search(p.name)
        if not match:
            return datetime.min
        return datetime.strptime(match.group(1), "%Y%m%d")

    return max(candidates, key=key_fn)


def ensure_table(engine, table: str) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id INT,
        customer_id INT,
        address VARCHAR(255),
        city VARCHAR(100),
        province VARCHAR(100),
        created_at DATETIME
    )
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def run() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    fallback_dir = Path(args.fallback_dir)

    latest_file = find_latest_file(input_dir, fallback_dir)
    df = pd.read_csv(latest_file)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    engine = create_engine(build_db_url())
    ensure_table(engine, args.table)
    df.to_sql(args.table, con=engine, if_exists="append", index=False)

    print(f"Ingested {len(df)} rows from {latest_file} into {args.table}.")


if __name__ == "__main__":
    run()
