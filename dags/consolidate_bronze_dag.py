from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# Configurações do ambiente
DB_PATH = "/Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3/data/projeto_etl/public_sales.db"
HDFS_BASE_PATH = "file:///Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3/data-lake/bronze/sales"

# Argumentos base para o Spark Submit
SPARK_ARGS_BASE = {
    "conn_id": "SPARK-BIGDATA",
    "application": "/Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3/dags/spark/load_hdfs.py",
    "jars": "/Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3/lib/sqlite-jdbc-3.44.1.0.jar",
    "conf": {"spark.master": "local[*]"},
    "verbose": True,
}

# ---------------------------------------------------------------------
# DEFINIÇÃO DAS TABELAS E SUAS ESTRATÉGIAS
# ---------------------------------------------------------------------

FULL_LOAD_TABLES = [
    {
        'table': 'customers',
        'query': 'SELECT customerid, firstname, lastname, email, phone FROM customers',
        'hdfs_path_suffix': '/customers/date={{ ds }}',
        'write_mode': 'overwrite'
    },
    {
        'table': 'products',
        'query': 'SELECT productid, productname, price, stockquantity FROM products',
        'hdfs_path_suffix': '/products/date={{ ds }}',
        'write_mode': 'overwrite'
    },
    {
        'table': 'orderitems',
        'query': 'SELECT orderitemid, orderid, productid, quantity, itemprice FROM orderitems',
        'hdfs_path_suffix': '/orderitems/date={{ ds }}',
        'write_mode': 'overwrite'
    },
]

INCREMENTAL_TABLES = [
    {
        'table': 'orders',
        # Query FINAL LIMPA: Sem CASTs, carrega a tabela inteira.
        'query': "SELECT orderid, customerid, orderdate, totalamount FROM orders",
        'hdfs_path_suffix': '/orders/date={{ ds }}',
        'write_mode': 'append'
    }
]

TABLES_TO_LOAD = FULL_LOAD_TABLES + INCREMENTAL_TABLES

# ---------------------------------------------------------------------
# DEFINIÇÃO DA DAG
# ---------------------------------------------------------------------

with DAG(
        dag_id='load_sales_bronze_layer',
        start_date=days_ago(1),
        schedule=None,
        catchup=False,
        tags=['etl', 'bronze', 'spark']
) as dag:
    start_task = EmptyOperator(task_id='start_carga_bronze')
    end_task = EmptyOperator(task_id='finalizar_carga_bronze')

    previous_task = start_task

    for table_config in TABLES_TO_LOAD:
        table_name = table_config['table']

        task_id = f'load_{table_name}_to_bronze'
        load_task = SparkSubmitOperator(
            task_id=task_id,
            name=task_id,
            application_args=[
                '--database', DB_PATH,
                '--table', table_name,
                '--query', table_config['query'],
                '--hdfs_path', HDFS_BASE_PATH + table_config['hdfs_path_suffix'],
                '--ds_date', '{{ ds }}',
                '--write_mode', table_config['write_mode'],
            ],
            **SPARK_ARGS_BASE
        )

        previous_task >> load_task
        previous_task = load_task

    previous_task >> end_task