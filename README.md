# Astroworld Data Engineering Test

## Overview

![Caption for the image](image-stack.png)

This repository contains a small data engineering project built with Apache Airflow and MySQL. The project bootstraps a MySQL warehouse, loads sample raw data, ingests daily customer address CSV files through Airflow, and provides SQL scripts for downstream cleaning and reporting.

The project is designed to be run locally with Docker Compose and to be easy to review end to end.

## Tech Stack

- Apache Airflow 2.9.3
- Python 3.11
- MySQL 8.0.45
- DBeaver
- pandas 2.1.4
- PyMySQL 1.1.1
- python-dotenv 1.0.1
- Docker Compose

## Repository Structure

- `dags/`
  - Airflow DAG definitions.
- `ingestion/`
  - Python ingestion script for customer address CSV files.
- `docker/mysql-init/`
  - MySQL initialization scripts for database creation, table creation, and seed data.
- `data/raw/customer_addresses/`
  - Daily raw CSV files used by the Airflow ingestion flow.
- `data_mart/`
  - SQL scripts for data cleaning and reporting tasks.
- `generate_dummy_customer_addresses.py`
  - Script to generate sample daily customer address CSV files.
- `docker-compose.yml`
  - Service definitions for MySQL and Airflow.
- `Dockerfile`
  - Custom Airflow image based on the official Airflow image.

## Project Flow

1. Docker Compose starts MySQL and Airflow services.
2. MySQL runs the init scripts in `docker/mysql-init/` on first startup with a fresh volume.
3. The init scripts create:
   - `airflow_metadata`
   - `warehouse`
   - MySQL users and privileges
   - raw warehouse tables
   - seed data for the non-address raw tables
4. Airflow runs the daily DAG from `dags/etl_pipeline.py`.
5. The DAG checks for a CSV file named `customer_addresses_YYYYMMDD.csv`.
6. The DAG appends the CSV data into `warehouse.customer_addresses_raw`.
7. SQL scripts in `data_mart/` can be executed to create cleaned tables and report outputs.

## Getting Started

### 1. Environment Configuration

Create or update `.env` with the expected local settings:

```env
DB_HOST=mysql
DB_PORT=3306
DB_USER=data_user
DB_PASSWORD=data_pass
DB_NAME=warehouse
DB_URL=mysql+pymysql://data_user:data_pass@mysql:3306/warehouse

AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow_metadata

AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_SECRET_KEY=supersecret
MYSQL_ROOT_PASSWORD=rootpass
```

### 2. Start the Project

For a normal startup:

```bash
docker compose up -d --build
```

For a full reset from scratch, including MySQL re-initialization:

```bash
docker compose down -v
docker compose pull mysql
docker compose up -d --build
```

Use the full reset flow when you want MySQL init scripts in `docker/mysql-init/` to run again from an empty database volume.

### 3. Generate Dummy CSV Files

If you need to regenerate sample customer address files:

```bash
python generate_dummy_customer_addresses.py
```

The generated files are placed under `data/raw/customer_addresses/`.

## Running Airflow

- Airflow UI: `http://localhost:8080`
- Default username: `admin`
- Default password: `admin`

The current active DAG is defined in [etl_pipeline.py](d:\Job\astroworld-data-engineering-test\dags\etl_pipeline.py) and builds `customer_address_ingest_1d`.

Current DAG behavior:

- Schedule: `@daily`
- Catchup: enabled
- Input naming convention: `customer_addresses_YYYYMMDD.csv`
- The DAG checks for a file based on the run interval and then loads it into `customer_addresses_raw`

Important note:

- The DAG expects the file for the computed business date to exist.
- If the expected CSV file is missing, the `check_data` task fails with `AirflowFailException`.

## MySQL Initialization and Seed Data

The MySQL service is defined in [docker-compose.yml](d:\Job\astroworld-data-engineering-test\docker-compose.yml) and uses `mysql:8.0.45`.

Initialization is handled by:

- [01-init.sql](d:\Job\astroworld-data-engineering-test\docker\mysql-init\01-init.sql)
- [02-create-warehouse-tables.sql](d:\Job\astroworld-data-engineering-test\docker\mysql-init\02-create-warehouse-tables.sql)
- [03-seed-warehouse-raw-data.sql](d:\Job\astroworld-data-engineering-test\docker\mysql-init\03-seed-warehouse-raw-data.sql)

These scripts run only when MySQL starts with a fresh data volume. They are not replayed on a normal container restart.

For SQL inspection and manual execution of data mart scripts, this project uses DBeaver to connect to the MySQL container.

## Running SQL Tasks

### Task 2a

Task 2a and Task 2b are intended to be executed manually in DBeaver against the MySQL container.

Use the following DBeaver connection settings based on the current `.env`:

- Host: `localhost`
- Port: `3306`
- Database: `warehouse`
- Username: `data_user`
- Password: `data_pass`

Steps to run Task 2a in DBeaver:

1. Make sure Docker Compose is running and the MySQL container is healthy.
2. Open DBeaver and create a new MySQL connection using the settings above.
3. Connect to the `warehouse` database.
4. Open [Task-2a.sql](d:\Job\astroworld-data-engineering-test\data_mart\Task-2a.sql).
5. Run the script to create cleaned tables from the raw warehouse tables.

Outputs include:

- `sales_clean`
- `customers_clean`
- `after_sales_clean`
- `customer_addresses_clean`

### Task 2b

Steps to run Task 2b in DBeaver:

1. Use the same DBeaver connection to the `warehouse` database.
2. Open [Task 2b.sql](d:\Job\astroworld-data-engineering-test\data_mart\Task%202b.sql).
3. Run the script after Task 2a has completed successfully.
4. Review the result sets returned by the queries.

Task 2b runs report queries on top of the cleaned data.

The script includes:

- a sales aggregation report
- an after-sales service priority report

## Troubleshooting

### `docker compose up` vs `up --build` vs `pull`

- `docker compose up`
  - Starts existing services using the images already available locally.
- `docker compose up --build`
  - Rebuilds services that use `build:` before starting them.
- `docker compose pull`
  - Downloads newer image tags from the registry without starting containers.

Use `docker compose pull mysql` if you change the pinned MySQL image version in Compose.

### DBeaver Cannot Connect to MySQL

Make sure the `mysql` service is already running before opening DBeaver. This project exposes MySQL on `localhost:3306`, so DBeaver will fail to connect if Docker Compose has not started the database container yet.
