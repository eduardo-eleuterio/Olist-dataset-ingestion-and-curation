import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging


def build_dm_vendas_clientes():
    logging.info(">>> Iniciando conexão com Postgres")
def build_dm_vendas_clientes():
    """
    Gera a tabela gold.dm_vendas_clientes com métricas agregadas por cliente.
    """
    import logging
    conn = None
    cur = None
    try:
        logging.info("Iniciando conexão com Postgres")
        conn = psycopg2.connect(
            host='postgres', 
            dbname='airflow',
            user='airflow',
            password='airflow'
        )
        cur = conn.cursor()

        # Cria schema gold se não existir
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        conn.commit()

        # Carregar tabelas necessárias da camada Silver
        customers = pd.read_sql('SELECT * FROM silver."olist_customers_dataset";', conn)
        orders = pd.read_sql('SELECT * FROM silver."olist_orders_dataset";', conn)
        order_items = pd.read_sql('SELECT * FROM silver."olist_order_items_dataset";', conn)
        products = pd.read_sql('SELECT * FROM silver."olist_products_dataset";', conn)

        # ====== Preparação ======
        # Merge clientes + pedidos
        df = orders.merge(customers, on="customer_id", how="left")
        df = df.merge(order_items, on="order_id", how="left")
        df = df.merge(products, on="product_id", how="left")

        # Converter para numérico antes da soma
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['freight_value'] = pd.to_numeric(df['freight_value'], errors='coerce')

        # Valor gasto = preço + frete
        df["valor_total_item"] = df["price"].fillna(0) + df["freight_value"].fillna(0)

        # Datas
        df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
        df["order_delivered_customer_date"] = pd.to_datetime(df["order_delivered_customer_date"], errors="coerce")

        # Data de referência global = última compra registrada
        data_referencia = df["order_purchase_timestamp"].max()

        # ====== Métricas por cliente ======
        agregacoes = {
            "order_id": pd.Series.nunique,
            "valor_total_item": "sum",
            "order_purchase_timestamp": ["min", "max"],
        }
        resumo = df.groupby("customer_unique_id").agg(agregacoes).reset_index()
        resumo.columns = [
            "customer_unique_id",
            "total_pedidos",
            "total_gasto",
            "data_primeira_compra",
            "data_ultima_compra",
        ]

        # Dias desde última compra
        resumo["dias_desde_ultima_compra"] = (
            (data_referencia - resumo["data_ultima_compra"]).dt.days
        )

        # Localização do cliente (pega do customers)
        loc = customers[["customer_unique_id", "customer_city", "customer_state"]].drop_duplicates()
        resumo = resumo.merge(loc, on="customer_unique_id", how="left")
        resumo.rename(
            columns={"customer_city": "cidade_cliente", "customer_state": "estado_cliente"},
            inplace=True
        )

        # Tempo médio de entrega (apenas pedidos entregues)
        entregas = df[df["order_delivered_customer_date"].notnull()].copy()
        entregas["tempo_entrega"] = (
            (entregas["order_delivered_customer_date"] - entregas["order_purchase_timestamp"]).dt.days
        )
        tempo_medio = entregas.groupby("customer_unique_id")["tempo_entrega"].mean().reset_index()
        tempo_medio.rename(columns={"tempo_entrega": "avg_delivery_time_days"}, inplace=True)
        resumo = resumo.merge(tempo_medio, on="customer_unique_id", how="left")

        # Categoria mais comprada (por gasto)
        gastos_categoria = (
            df.groupby(["customer_unique_id", "product_category_name"])["valor_total_item"]
            .sum()
            .reset_index()
        )
        idx = gastos_categoria.groupby("customer_unique_id")["valor_total_item"].idxmax()
        categoria_top = gastos_categoria.loc[idx, ["customer_unique_id", "product_category_name"]]
        categoria_top.rename(columns={"product_category_name": "categoria_mais_comprada"}, inplace=True)
        resumo = resumo.merge(categoria_top, on="customer_unique_id", how="left")

        # Garantir ordem e tipo das colunas
        resumo = resumo[
            [
                "customer_unique_id",
                "estado_cliente",
                "cidade_cliente",
                "total_pedidos",
                "total_gasto",
                "data_primeira_compra",
                "data_ultima_compra",
                "dias_desde_ultima_compra",
                "avg_delivery_time_days",
                "categoria_mais_comprada"
            ]
        ]
        resumo["total_pedidos"] = resumo["total_pedidos"].astype(int)
        resumo["total_gasto"] = resumo["total_gasto"].astype(float)

        logging.info(f"Iniciando carga gold: {len(resumo)} registros para inserir")

        # ====== Criar tabela no schema gold ======
        cur.execute('DROP TABLE IF EXISTS gold."dm_vendas_clientes";')
        cur.execute("""
CREATE TABLE gold."dm_vendas_clientes" (
    customer_unique_id TEXT,
    estado_cliente TEXT,
    cidade_cliente TEXT,
    total_pedidos INTEGER,
    total_gasto FLOAT,
    data_primeira_compra TIMESTAMP,
    data_ultima_compra TIMESTAMP,
    dias_desde_ultima_compra INTEGER,
    avg_delivery_time_days FLOAT,
    categoria_mais_comprada TEXT
);
""")

        # Inserir dados
        values = resumo.where(pd.notnull(resumo), None).values.tolist()
        execute_values(
            cur,
            'INSERT INTO gold."dm_vendas_clientes" VALUES %s',
            values
        )

        conn.commit()
        logging.info(f">>> Inseridos {len(values)} registros na tabela gold.dm_vendas_clientes")
    except Exception as e:
        logging.error(f"Erro na carga gold: {e}")
        raise
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


# ====== DAG do Airflow ======
default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'depends_on_past': False
}

with DAG(
    dag_id='carga_gold',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    gold_task = PythonOperator(
        task_id='build_dm_vendas_clientes',
        python_callable=build_dm_vendas_clientes,
        execution_timeout=timedelta(minutes=10),
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
