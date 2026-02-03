import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
import os
from urllib.parse import quote_plus

# --- 1. CONFIG ---
DB_USER = "postgres"
DB_PASS = "WWWWWWWWWWWWW"  # <---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"

project_id = "postgresql-to-bq-test-485807"
dataset_id = "postgresql_to_bq_test_mock_data_py_generator"

# CAMBIO 1:Name updated
main_table_name = "clientes_mock"

# --- 2. CONNECTION ---
encoded_pass = quote_plus(DB_PASS)
db_connection_str = (
    f"postgresql+psycopg2://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
db_connection = create_engine(
    db_connection_str, connect_args={"client_encoding": "utf8"}
)

main_table_id = f"{project_id}.{dataset_id}.{main_table_name}"


def run_append_etl():
    print(f"--- INICIANDO ETL: APPEND A '{main_table_name}' ---")

    # --- PASO 1: EXTRAER DE POSTGRES ---
    try:
        print("1. Leyendo PostgreSQL...")
        df = pd.read_sql("SELECT * FROM public.clientes_mock", db_connection)

        # Transformaciones
        df["total_spend"] = df["total_spend"].astype(float)
        df["signup_date"] = pd.to_datetime(df["signup_date"])
        print(f"   > Se encontraron {len(df)} filas nuevas en tu PC.")

    except Exception as e:
        print(f"Error Postgres: {e}")
        return

    # --- PASO 2: LOAD TO BIGQUERY (APPEND) ---
    client = bigquery.Client(project=project_id)

    try:
        print("2. Enviando datos a BigQuery...")
        job_config = bigquery.LoadJobConfig(
            # CAMBIO 2: WRITE_APPEND
            write_disposition="WRITE_APPEND",
        )
        job = client.load_table_from_dataframe(df, main_table_id, job_config=job_config)
        job.result()  # Wait for the job to complete.

        # Final veritifcation
        table = client.get_table(main_table_id)
        print(f"¡ÉXITO! Carga completada.")
        print(
            f"Tu tabla '{main_table_name}' ahora tiene un total de {table.num_rows} filas."
        )

    except Exception as e:
        print(f"Error en BigQuery: {e}")


if __name__ == "__main__":
    run_append_etl()
