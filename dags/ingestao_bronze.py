from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

DATA_PATH = '/data'  # ajuste se necessário
CSV_FILES = [
    'olist_customers_dataset.csv',
    'olist_geolocation_dataset.csv',
    'olist_order_items_dataset.csv',
    'olist_order_payments_dataset.csv',
    'olist_order_reviews_dataset.csv',
    'olist_orders_dataset.csv',
    'olist_products_dataset.csv',
    'olist_sellers_dataset.csv',
    'product_category_name_translation.csv'
]

def ingest_csv_to_postgres(file_name, **kwargs):
    conn = psycopg2.connect(
        host='postgres',  # nome do serviço no docker-compose
        dbname='airflow',
        user='airflow',
        password='airflow'
    )
    table_name = file_name.replace('.csv', '')
    schema = 'bronze'
    file_path = os.path.join(DATA_PATH, file_name)
    df = pd.read_csv(file_path)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    with conn.cursor() as cur:
        # Cria schema se não existir
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        # Cria tabela (drop se existir para idempotência)
        cur.execute(f'DROP TABLE IF EXISTS {schema}."{table_name}";')
        columns = ', '.join([f'"{col}" TEXT' for col in df.columns])
        cur.execute(f'CREATE TABLE {schema}."{table_name}" ({columns});')
        # Insere dados
        for row in df.itertuples(index=False, name=None):
            values = ', '.join(["'" + str(val).replace("'", "''") + "'" if pd.notnull(val) else 'NULL' for val in row])
            cur.execute(f'INSERT INTO {schema}."{table_name}" VALUES ({values});')
        conn.commit()
    conn.close()

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='ingestao_bronze',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    tasks = []
    for file_name in CSV_FILES:
        task = PythonOperator(
            task_id=f'ingest_{file_name.replace(".csv", "")}',
            python_callable=ingest_csv_to_postgres,
            op_kwargs={'file_name': file_name}
        )
        tasks.append(task)