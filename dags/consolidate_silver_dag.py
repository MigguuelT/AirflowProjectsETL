from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# --- CONFIGURAÇÕES ---
# Ajuste o caminho base conforme seu ambiente
BASE_PROJECT_PATH = "/Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3"
# Definindo BRONZE_ROOT sem depender de F-STRING:
BRONZE_ROOT = "file://" + BASE_PROJECT_PATH + "/data-lake/bronze/sales"
SILVER_ROOT = "file://" + BASE_PROJECT_PATH + "/data-lake/silver/sales"

SCRIPT_PATH = f"{BASE_PROJECT_PATH}/dags/spark/process_silver.py"
JAR_PATH = f"{BASE_PROJECT_PATH}/lib/sqlite-jdbc-3.44.1.0.jar"

SPARK_CONF = {
    "conn_id": "SPARK-BIGDATA",
    "application": SCRIPT_PATH,
    "jars": JAR_PATH,
    "conf": {"spark.master": "local[*]"},
    "verbose": True
}

with DAG(
    dag_id='consolidate_silver_layer',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'silver', 'spark']
) as dag:

    start_task = EmptyOperator(task_id='start_silver')
    end_task = EmptyOperator(task_id='end_silver')

    # 1. Customers (Full Load - Deduplicação)
    process_customers = SparkSubmitOperator(
        task_id='process_customers_silver',
        name='silver_customers',
        application_args=[
            '--table_name', 'customers',
            '--bronze_path', BRONZE_ROOT + "/customers",
            '--silver_path', SILVER_ROOT + "/customers",
            '--primary_key', 'customerid'
        ],
        **SPARK_CONF
    )

    # 2. Products (Full Load - Deduplicação)
    process_products = SparkSubmitOperator(
        task_id='process_products_silver',
        name='silver_products',
        application_args=[
            '--table_name', 'products',
            '--bronze_path', BRONZE_ROOT + "/products",
            '--silver_path', SILVER_ROOT + "/products",
            '--primary_key', 'productid'
        ],
        **SPARK_CONF
    )

    # 3. OrderItems (Full Load - Deduplicação)
    process_orderitems = SparkSubmitOperator(
        task_id='process_orderitems_silver',
        name='silver_orderitems',
        application_args=[
            '--table_name', 'orderitems',
            '--bronze_path', BRONZE_ROOT + "/orderitems",
            '--silver_path', SILVER_ROOT + "/orderitems",
            '--primary_key', 'orderitemid'
        ],
        **SPARK_CONF
    )

    # 4. Orders (Incremental - Apenas move a partição do dia)
    process_orders = SparkSubmitOperator(
        task_id='process_orders_silver',
        name='silver_orders',
        application_args=[
            '--table_name', 'orders',
            # ✅ CORREÇÃO: Usando CONCATENAÇÃO SIMPLES para forçar o Airflow a ver
            # o '{{ ds }}' como uma variável Jinja, e não uma f-string Python.
            '--bronze_path', BRONZE_ROOT + "/orders/date={{ ds }}",
            '--silver_path', SILVER_ROOT + "/orders",
            '--is_incremental'
        ],
        **SPARK_CONF
    )

    # Fluxo
    start_task >> process_customers >> process_products >> process_orderitems >> process_orders >> end_task