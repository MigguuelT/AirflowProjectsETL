from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3'


def gera_arquivo():
    with open(AIRFLOW_HOME + '/data/arquivo.txt', 'w') as f:
        for i in range(10):
            f.write('Arquivo texto linha {}\n'.format(i))


def processa_arquivo():
    arquivo = ""
    with open(AIRFLOW_HOME + '/data/arquivo.txt', 'r') as f:
        for line in f:
            arquivo += line.strip('\n') + ' - Processado com sucesso!\n'

    with open(AIRFLOW_HOME + '/data/arquivo_processado.txt', 'w') as f:
        f.write(arquivo)


with DAG(
    dag_id = 'dag_exemplo',
    schedule = None,
    start_date = datetime(2020, 1, 1),
) as dag:

    gera_arquivo = PythonOperator(
        task_id = 'gera_arquivo',
        python_callable = gera_arquivo,
    )

    processa_arquivo = PythonOperator(
        task_id = 'processa_arquivo',
        python_callable = processa_arquivo,
    )

    gera_arquivo >> processa_arquivo

    # dag.test()