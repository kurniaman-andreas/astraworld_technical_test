import pandas as pd
from datetime import datetime, timedelta
import os
import random

# Config
OUTPUT_DIR = "data/raw/customer_addresses"
BASE_DATE = datetime(2026, 3, 1)
NUM_FILES = 3
ROWS_PER_FILE = 4

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Data pool sesuai sample
ADDRESS_POOL = [
    (1, "Jalan Mawar V, RT 1/RW 2", "Bekasi", "Jawa Barat"),
    (2, "Jl. Melati 7 No 12", "Depok", "Jawa Barat"),
    (3, "Jl Ababil Indah", "Tangerang Selatan", "Jawa Barat"),
    (4, "Jl. Kemang Raya 1 No 3", "JAKARTA PUSAT", "DKI JAKARTA"),
    (5, "Jl. Cendana 3 No 8", "Bandung", "Jawa Barat"),
    (6, "Astra Tower Jalan Yos Sudarso 12", "Jakarta Utara", "DKI Jakarta"),
]


def generate_data(current_date, rows):
    selected = random.sample(ADDRESS_POOL, rows)

    created_at = current_date.strftime("%Y-%m-%d") + " 14:24:40.012"

    return {
        "id": list(range(1, rows + 1)),
        "customer_id": [row[0] for row in selected],
        "address": [row[1] for row in selected],
        "city": [row[2] for row in selected],
        "province": [row[3] for row in selected],
        "created_at": [created_at] * rows
    }


for i in range(NUM_FILES):
    current_date = BASE_DATE + timedelta(days=i)
    date_str = current_date.strftime("%Y%m%d")

    data = generate_data(current_date, ROWS_PER_FILE)
    df = pd.DataFrame(data)

    filepath = f"{OUTPUT_DIR}/customer_addresses_{date_str}.csv"

    df.to_csv(filepath, index=False)

    print(f"Generated: {filepath}")

