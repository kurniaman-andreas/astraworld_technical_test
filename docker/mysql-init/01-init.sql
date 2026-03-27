CREATE DATABASE IF NOT EXISTS airflow_metadata;
CREATE DATABASE IF NOT EXISTS warehouse;

CREATE USER IF NOT EXISTS 'airflow'@'%' IDENTIFIED BY 'airflow';
CREATE USER IF NOT EXISTS 'data_user'@'%' IDENTIFIED BY 'data_pass';

GRANT ALL PRIVILEGES ON airflow_metadata.* TO 'airflow'@'%';
GRANT ALL PRIVILEGES ON warehouse.* TO 'data_user'@'%';
FLUSH PRIVILEGES;
