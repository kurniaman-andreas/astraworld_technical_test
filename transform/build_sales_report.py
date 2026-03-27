import os

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


def run() -> None:
    engine = create_engine(build_db_url())

    query = """
    SELECT
        DATE_FORMAT(invoice_date, '%Y-%m') AS periode,
        CASE
            WHEN price_num BETWEEN 100000000 AND 249999999 THEN 'LOW'
            WHEN price_num BETWEEN 250000000 AND 400000000 THEN 'MEDIUM'
            WHEN price_num > 400000000 THEN 'HIGH'
            ELSE 'UNKNOWN'
        END AS class,
        model,
        SUM(price_num) AS total
    FROM (
        SELECT
            model,
            invoice_date,
            CAST(REGEXP_REPLACE(price, '[^0-9]', '') AS UNSIGNED) AS price_num
        FROM sales_raw
    ) s
    GROUP BY periode, class, model
    ORDER BY periode, class, model
    """

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dm_sales_report"))
        conn.execute(
            text(
                """
                CREATE TABLE dm_sales_report (
                    periode VARCHAR(7),
                    class VARCHAR(10),
                    model VARCHAR(50),
                    total BIGINT
                )
                """
            )
        )
        conn.execute(text(f"INSERT INTO dm_sales_report {query}"))

    print("Built dm_sales_report.")


if __name__ == "__main__":
    run()
