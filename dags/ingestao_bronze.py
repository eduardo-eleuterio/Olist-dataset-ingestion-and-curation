from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DATA_PATH = '/data' 
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
    """
    Ingesta um arquivo CSV para o Postgres na camada bronze, validando schema, idempotência e logando etapas.
    """
    import logging
    conn = None
    try:
        table_name = file_name.replace('.csv', '')
        schema = 'bronze'
        file_path = os.path.join(DATA_PATH, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        logging.info(f"Lendo arquivo {file_path}")
        df = pd.read_csv(file_path, encoding='utf-8')
        if df.empty:
            logging.warning(f"Arquivo {file_name} está vazio. Pulando ingestão.")
            return
        # Validação de schema: checa se tem colunas
        if len(df.columns) == 0:
            raise ValueError(f"Arquivo {file_name} não possui colunas.")
        conn = psycopg2.connect(
            host='postgres', 
            dbname='airflow',
            user='airflow',
            password='airflow'
        )
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            # Idempotência: checa se já existe dados
            cur.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_schema='{schema}' AND table_name='{table_name}';")
            exists = cur.fetchone()[0]
            if exists:
                logging.info(f"Tabela {schema}.{table_name} já existe. Removendo para reprocessar.")
                cur.execute(f'DROP TABLE IF EXISTS {schema}."{table_name}";')
            columns = ', '.join([f'"{col}" TEXT' for col in df.columns])
            cur.execute(f'CREATE TABLE {schema}."{table_name}" ({columns});')
            logging.info(f"Iniciando ingestão de {len(df)} registros em {schema}.{table_name}")
            for row in df.itertuples(index=False, name=None):
                values = ', '.join(["'" + str(val).replace("'", "''") + "'" if pd.notnull(val) else 'NULL' for val in row])
                cur.execute(f'INSERT INTO {schema}."{table_name}" VALUES ({values});')
            conn.commit()
            logging.info(f"Ingestão finalizada para {schema}.{table_name}")
    except Exception as e:
        logging.error(f"Erro na ingestão bronze de {file_name}: {e}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

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

trigger_silver = TriggerDagRunOperator(
    task_id='trigger_silver',
    trigger_dag_id='processamento_silver'
)

tasks[-1] >> trigger_silver