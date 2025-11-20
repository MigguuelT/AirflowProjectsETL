from collections import namedtuple
from datetime import datetime

import pytest
from airflow import DAG

pytest_plugins = ["helpers_namespace"]


@pytest.helpers.register
def run_task(task, dag):
    dag.clear()
    task.run(
        start_date=dag.default_args["start_date"],
    )

@pytest.fixture
def test_dag():
    return DAG(
        dag_id="test_dag",
        default_args={"owner": "airflow", "start_date": datetime(2021, 1, 1)},
        schedule=None
    )
