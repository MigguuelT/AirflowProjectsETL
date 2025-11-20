import pytest
from airflow.operators.python import PythonOperator


def save_file(path):
    with open(path, "w") as f:
        for i in range(5):
            f.write(f'teste{i}')

def test_savefile(test_dag, tmpdir):
    tmpfile = tmpdir.join('test.txt')

    task = PythonOperator(
        task_id='save_file',
        python_callable=save_file,
        op_kwargs={"path": tmpfile},
        dag=test_dag
    )

    pytest.helpers.run_task(task, test_dag)

    assert len(tmpdir.listdir()) == 1
    assert tmpfile.read() == "teste0teste1teste2teste3teste4"
