from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

import pandas as pd
import psycopg2
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

def process_silver():
    conn = psycopg2.connect(
        host='postgres',
        dbname='airflow',
        user='airflow',
        password='airflow'
    )
    schema_bronze = 'bronze'
    schema_silver = 'silver'
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_silver};")

        # 1. olist_orders_dataset: Tipagem e tratamento de nulos
        df_orders = pd.read_sql(f'SELECT * FROM {schema_bronze}."olist_orders_dataset"', conn)
        df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])
        df_orders['order_approved_at'] = pd.to_datetime(df_orders['order_approved_at'])
        df_orders['order_delivered_customer_date'] = pd.to_datetime(df_orders['order_delivered_customer_date'])
        # Estratégia: manter nulo para pedidos não entregues (permite calcular tempo só para entregues)
        # Salva no silver
        cur.execute(f'DROP TABLE IF EXISTS {schema_silver}."olist_orders_dataset";')
        columns = ', '.join([f'"{col}" TIMESTAMP' if 'timestamp' in col or 'date' in col else f'"{col}" TEXT' for col in df_orders.columns])
        cur.execute(f'CREATE TABLE {schema_silver}."olist_orders_dataset" ({columns});')
        for row in df_orders.itertuples(index=False, name=None):
            values = ', '.join([f"'{val}'" if pd.notnull(val) else 'NULL' for val in row])
            cur.execute(f'INSERT INTO {schema_silver}."olist_orders_dataset" VALUES ({values});')

        # 2. olist_customers_dataset: Sanitização com regex
        df_customers = pd.read_sql(f'SELECT * FROM {schema_bronze}."olist_customers_dataset"', conn)
        df_customers['customer_zip_code_prefix'] = df_customers['customer_zip_code_prefix'].astype(str).str.extract(r'(\\d{5})')[0]
        cur.execute(f'DROP TABLE IF EXISTS {schema_silver}."olist_customers_dataset";')
        columns = ', '.join([f'"{col}" TEXT' for col in df_customers.columns])
        cur.execute(f'CREATE TABLE {schema_silver}."olist_customers_dataset" ({columns});')
        for row in df_customers.itertuples(index=False, name=None):
            values = ', '.join([f"'{val}'" if pd.notnull(val) else 'NULL' for val in row])
            cur.execute(f'INSERT INTO {schema_silver}."olist_customers_dataset" VALUES ({values});')

        # 3. olist_products_dataset: Normalização de textos
        df_products = pd.read_sql(f'SELECT * FROM {schema_bronze}."olist_products_dataset"', conn)
        df_products['product_category_name'] = df_products['product_category_name'].str.strip().str.lower()
        cur.execute(f'DROP TABLE IF EXISTS {schema_silver}."olist_products_dataset";')
        columns = ', '.join([f'"{col}" TEXT' for col in df_products.columns])
        cur.execute(f'CREATE TABLE {schema_silver}."olist_products_dataset" ({columns});')
        for row in df_products.itertuples(index=False, name=None):
            values = ', '.join([f"'{val}'" if pd.notnull(val) else 'NULL' for val in row])
            cur.execute(f'INSERT INTO {schema_silver}."olist_products_dataset" VALUES ({values});')

        # 4. olist_order_items_dataset: Deduplicação
        df_items = pd.read_sql(f'SELECT * FROM {schema_bronze}."olist_order_items_dataset"', conn)
        df_items = df_items.drop_duplicates(subset=['order_id', 'order_item_id'])
        cur.execute(f'DROP TABLE IF EXISTS {schema_silver}."olist_order_items_dataset";')
        columns = ', '.join([f'"{col}" TEXT' for col in df_items.columns])
        cur.execute(f'CREATE TABLE {schema_silver}."olist_order_items_dataset" ({columns});')
        for row in df_items.itertuples(index=False, name=None):
            values = ', '.join([f"'{val}'" if pd.notnull(val) else 'NULL' for val in row])
            cur.execute(f'INSERT INTO {schema_silver}."olist_order_items_dataset" VALUES ({values});')

        # Repita para as demais tabelas, apenas copiando do bronze para o silver se não houver transformação

        conn.commit()
    conn.close()

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='processamento_silver',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    wait_bronze = ExternalTaskSensor(
        task_id='wait_ingestao_bronze',
        external_dag_id='ingestao_bronze',
        external_task_id=None,  # espera a DAG inteira
        mode='poke',
        timeout=600
    )

    silver_task = PythonOperator(
        task_id='process_silver',
        python_callable=process_silver
    )

    wait_bronze >> silver_task

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='processamento_silver',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    silver_task = PythonOperator(
        task_id='process_silver',
        python_callable=process_silver,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )