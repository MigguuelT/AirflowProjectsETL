from datetime import datetime
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# --- CONFIGURAÇÕES ---
BASE_PATH = "/Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3"
SILVER_BASE = f"file://{BASE_PATH}/data-lake/silver/sales"
GOLD_BASE = f"file://{BASE_PATH}/data-lake/gold/sales"
SCRIPT_PATH = f"{BASE_PATH}/dags/spark/process_gold.py"
JAR_PATH = f"{BASE_PATH}/lib/sqlite-jdbc-3.44.1.0.jar"

SPARK_ARGS_BASE = {
    "conn_id": "SPARK-BIGDATA",
    "application": SCRIPT_PATH,
    "jars": JAR_PATH,
    "conf": {"spark.master": "local[*]"},
    "verbose": True,
}

with DAG(
    dag_id='consolidate_gold_layer',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'gold', 'spark']
) as dag:

    process_gold = SparkSubmitOperator(
        task_id='process_sales_gold',
        application_args=[
            '--silver_base_path', SILVER_BASE,
            '--gold_base_path', GOLD_BASE,
            '--ds_date', '{{ ds }}'
        ],
        **SPARK_ARGS_BASE
    )