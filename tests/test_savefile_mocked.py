from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG


def save_file(path):
    with open(path, "w") as f:
        for i in range(5):
            f.write(f'teste{i}')


# NOTA: Removemos 'tmpdir' pois não vamos tocar no disco.
def test_savefile_mocked(test_dag: DAG, mocker):
    expected_path = "/caminho/ficticio/test.txt"

    # 2. Mockar a função 'open' nativa do Python
    # Isso intercepta o "with open(path, 'w')"
    mock_open = mocker.patch("builtins.open")

    # 3. Definição da Task
    task = PythonOperator(
        task_id='save_file',
        python_callable=save_file,
        op_kwargs={"path": expected_path},
        dag=test_dag  # Mantemos a DAG apenas para o contexto do Airflow
    )

    # 4. Execução da Task (o código é executado, mas o 'open' é falso)
    # Usamos o helper de execução simples que criamos
    task.execute(context={})

    # --- ASSERÇÕES COM MOCK ---

    # A. Verifica se 'open' foi chamado uma vez com os argumentos corretos
    mock_open.assert_called_once_with(expected_path, "w")

    # B. Captura o objeto de arquivo (file handle) simulado que a função usou
    # A estrutura: open.return_value.__enter__.return_value representa o 'f' em 'with open(...) as f'
    mock_file_handle = mock_open.return_value.__enter__.return_value

    # C. Verifica se o metodo 'write' foi chamado o número correto de vezes (5 vezes)
    assert mock_file_handle.write.call_count == 5

    # D. Verifica o conteúdo exato que seria escrito
    mock_file_handle.write.assert_has_calls([
        mocker.call("teste0"),
        mocker.call("teste1"),
        mocker.call("teste2"),
        mocker.call("teste3"),
        mocker.call("teste4"),
    ])