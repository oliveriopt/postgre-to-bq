import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
import os
from urllib.parse import quote_plus

# --- Config Neon ---
DB_USER = "neondb_owner"
DB_NAME = "neondb"
DB_PORT = "5432"

# Neon Secrets (Seguridad Máxima)
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

# Config to BigQuery
project_id = "postgresql-to-bq-test-485807"
dataset_id = "postgresql_to_bq_test_mock_data_py_generator"
table_name = "clientes_mock"


def run_cloud_etl():
    print("--- INITIATING ETL: GITHUB ACTIONS -> NEON -> BIGQUERY ---")

    if not DB_PASS or not DB_HOST:
        print("❌ Error: DB_PASSWORD or DB_HOST doesn't found it.")
        return

    try:
        # 1. CONNECTION TO NEON
        print("1. Connecting to Neon PostgreSQL...")
        encoded_pass = quote_plus(DB_PASS)
        # Neon requiere sslmode=require
        db_connection_str = f"postgresql+psycopg2://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

        db_connection = create_engine(db_connection_str)

        # 2. EXTRACTION
        print(f"   > Reading table '{table_name}'...")
        df = pd.read_sql(f"SELECT * FROM {table_name}", db_connection)

        if df.empty:
            print("⚠️ The table in Neon is empty. Nothing to upload")
            return

        print(f"   > Rows downloaded {len(df)}.")

        # 3. LOAD TO BIGQUERY
        print("2. Connecting to BigQuery...")
        client = bigquery.Client(project=project_id)
        table_full_id = f"{project_id}.{dataset_id}.{table_name}"

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

        print("   > Uploading data...")
        job = client.load_table_from_dataframe(df, table_full_id, job_config=job_config)
        job.result()  # Esperar a que termine

        print("✅ ¡SUCESS! Pipeline finalized correctly.")

    except Exception as e:
        print(f"❌ Critical error in the process: {e}")
        # Hacemos que falle el Action si hay error
        exit(1)


if __name__ == "__main__":
    run_cloud_etl()
