-- Airflow needs its own metadata database; it must not share retail_src.
-- Runs only on first initialisation of the Postgres data volume.
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
