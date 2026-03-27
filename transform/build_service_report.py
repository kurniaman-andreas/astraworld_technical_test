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
        DATE_FORMAT(a.service_date, '%Y') AS periode,
        a.vin,
        c.name AS customer_name,
        COALESCE(addr.address, '-') AS address,
        COUNT(*) AS count_service,
        CASE
            WHEN COUNT(*) > 10 THEN 'HIGH'
            WHEN COUNT(*) BETWEEN 5 AND 10 THEN 'MED'
            ELSE 'LOW'
        END AS priority
    FROM after_sales_raw a
    LEFT JOIN customers_raw c
        ON a.customer_id = c.id
    LEFT JOIN customer_addresses_clean addr
        ON a.customer_id = addr.customer_id
    GROUP BY
        DATE_FORMAT(a.service_date, '%Y'),
        a.vin,
        c.name,
        addr.address
    ORDER BY periode, count_service DESC
    """

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dm_service_report"))
        conn.execute(
            text(
                """
                CREATE TABLE dm_service_report (
                    periode VARCHAR(4),
                    vin VARCHAR(30),
                    customer_name VARCHAR(100),
                    address VARCHAR(255),
                    count_service INT,
                    priority VARCHAR(10)
                )
                """
            )
        )
        conn.execute(text(f"INSERT INTO dm_service_report {query}"))

    print("Built dm_service_report.")


if __name__ == "__main__":
    run()
