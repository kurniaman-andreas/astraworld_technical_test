import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


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


def normalize_province(value: str) -> str:
    if value is None:
        return None
    value = value.strip()
    if value.lower() == "dki jakarta":
        return "DKI Jakarta"
    return value.title()


def run() -> None:
    engine = create_engine(build_db_url())
    df = pd.read_sql("SELECT * FROM customer_addresses_raw", con=engine)

    df["address"] = df["address"].astype(str).str.strip()
    df["city"] = df["city"].astype(str).str.strip().str.title()
    df["province"] = df["province"].astype(str).apply(normalize_province)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    df = (
        df.sort_values("created_at")
        .drop_duplicates(subset=["customer_id"], keep="last")
        .reset_index(drop=True)
    )

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS customer_addresses_clean"))

    df.to_sql("customer_addresses_clean", con=engine, if_exists="replace", index=False)
    print(f"Cleaned customer_addresses_raw -> customer_addresses_clean ({len(df)} rows).")


if __name__ == "__main__":
    run()
