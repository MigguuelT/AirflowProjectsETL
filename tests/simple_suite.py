import os

import pytest
from airflow.operators.python import PythonOperator

from tests.tasks_dag import le_arquivo, mascara_senha, salva_arquivo

AIRFLOW_HOME= '/Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3'

def test_file_exist():
    assert os.path.exists(f'{AIRFLOW_HOME}/data/novo.csv')

def test_read():
    df = le_arquivo()
    assert df.shape == (9,4)

def test_masking():
    df = le_arquivo()
    df = mascara_senha(df)
    assert df.iloc[0, 2] == '****'

def test_task(test_dag):

    def task_definition():
        df = le_arquivo()
        df = mascara_senha(df)
        salva_arquivo(df)

        task = PythonOperator(
            task_id='mascara_senha',
            python_callable=task_definition,
            dag=test_dag
        )

        pytest.helpers.run_task(task, test_dag)

        assert os.path.exists(f'{AIRFLOW_HOME}/data/novo_final.csv')