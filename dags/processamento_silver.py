import pandas as pd
from sqlalchemy import create_engine
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def process_table(table_name, schema_bronze, schema_silver, engine, transform_func=None, dedup_cols=None, type_map=None):
    df = pd.read_sql(f'SELECT * FROM {schema_bronze}."{table_name}"', engine)
    if transform_func:
        df = transform_func(df)
    if dedup_cols:
        df = df.drop_duplicates(subset=dedup_cols)
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.execute(f'DROP TABLE IF EXISTS {schema_silver}."{table_name}";')
    if type_map:
        columns = ', '.join([f'"{col}" {type_map.get(col, "TEXT")}' for col in df.columns])
    else:
        columns = ', '.join([f'"{col}" TEXT' for col in df.columns])
    cur.execute(f'CREATE TABLE {schema_silver}."{table_name}" ({columns});')
    for row in df.itertuples(index=False, name=None):
        value_list = []
        for val in row:
            if pd.notnull(val):
                safe_val = str(val).replace("'", "''")
                value_list.append("'" + safe_val + "'")
            else:
                value_list.append('NULL')
        values = ', '.join(value_list)
        cur.execute(f'INSERT INTO {schema_silver}."{table_name}" VALUES ({values});')
    conn.commit()
    cur.close()
    conn.close()

def process_silver():
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
    schema_bronze = 'bronze'
    schema_silver = 'silver'
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_silver};")
    cur.close()
    conn.close()

    def deduplicate(df, subset):
        """Remove duplicados do DataFrame com base nas colunas fornecidas."""
        return df.drop_duplicates(subset=subset)

    def transform_orders(df):
        df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
        df['order_approved_at'] = pd.to_datetime(df['order_approved_at'])
        df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'])
        df = deduplicate(df, ['order_id'])
        return df
    type_map_orders = {col: 'TIMESTAMP' if 'timestamp' in col or 'date' in col else 'TEXT' for col in ['order_id','customer_id','order_status','order_purchase_timestamp','order_approved_at','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date']}
    process_table(
        table_name='olist_orders_dataset',
        schema_bronze=schema_bronze,
        schema_silver=schema_silver,
        engine=engine,
        transform_func=transform_orders,
        type_map=type_map_orders
    )

    def transform_customers(df):
        df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str).str.extract(r'(\d{5})')[0]
        df = deduplicate(df, ['customer_unique_id'])
        return df
    process_table(
        table_name='olist_customers_dataset',
        schema_bronze=schema_bronze,
        schema_silver=schema_silver,
        engine=engine,
        transform_func=transform_customers
    )

    def transform_products(df):
        df['product_category_name'] = df['product_category_name'].str.strip().str.lower()
        df = deduplicate(df, ['product_id'])
        return df
    process_table(
        table_name='olist_products_dataset',
        schema_bronze=schema_bronze,
        schema_silver=schema_silver,
        engine=engine,
        transform_func=transform_products
    )

    process_table(
        table_name='olist_order_items_dataset',
        schema_bronze=schema_bronze,
        schema_silver=schema_silver,
        engine=engine,
        dedup_cols=['order_id', 'order_item_id']
    )

    for tb in [
        'olist_geolocation_dataset',
        'olist_order_payments_dataset',
        'olist_order_reviews_dataset',
        'olist_sellers_dataset',
        'product_category_name_translation'
    ]:
        # Definir colunas de deduplicação para cada tabela
        dedup_map = {
            'olist_geolocation_dataset': ['geolocation_zip_code_prefix'],
            'olist_order_payments_dataset': ['order_id', 'payment_sequential'],
            'olist_order_reviews_dataset': ['review_id'],
            'olist_sellers_dataset': ['seller_id'],
            'product_category_name_translation': ['product_category_name']
        }
        dedup_cols = dedup_map.get(tb)
        def transform_generic(df, dedup_cols=dedup_cols):
            if dedup_cols:
                df = deduplicate(df, dedup_cols)
            return df
        process_table(
            table_name=tb,
            schema_bronze=schema_bronze,
            schema_silver=schema_silver,
            engine=engine,
            transform_func=transform_generic
        )

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

trigger_gold = TriggerDagRunOperator(
    task_id='trigger_gold',
    trigger_dag_id='carga_gold'
)

silver_task >> trigger_gold