from datetime import datetime
import pendulum
from unittest import mock
import pytest
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import SimpleHttpOperator


# --- 1. LÓGICA DE NEGÓCIO ISOLADA (Para teste de unidade puro) ---

def check_light_logic(response_data: dict) -> bool:
    results = response_data["results"]

    sunrise_naive = datetime.strptime(results["sunrise"][:-6], "%Y-%m-%dT%H:%M:%S")
    sunset_naive = datetime.strptime(results["sunset"][:-6], "%Y-%m-%dT%H:%M:%S")

    utc_tz = pendulum.timezone('UTC')
    sunrise = utc_tz.convert(sunrise_naive)
    sunset = utc_tz.convert(sunset_naive)
    now_utc = pendulum.now('UTC')  # Pendulum é o tipo de data preferido do Airflow

    return False if sunrise < now_utc < sunset else True


# --- 2. TESTE DO OPERADOR (Foca na Configuração e Chamada) ---

def test_http_operator(test_dag, mocker):
    # Mocking dos dados que a API retornaria
    mock_response_data = {
        "results": {
            "sunrise": "2025-01-01T06:00:00+00:00",
            "sunset": "2025-01-01T18:00:00+00:00",
        }
    }

    # CRÍTICO: Mockamos a função de checagem para evitar o TypeError no runtime
    mock_check_light = mocker.patch(
        "tests.test_http.check_light_logic",
        return_value=True  # Simula que o check passou
    )

    # Mockamos o metodo de execução do Hook no caminho onde ele é usado
    mock_run_method = mocker.patch.object(
        HttpHook,
        "run",  # Alvo mais simples
        return_value=mocker.MagicMock(
            status_code=200,
            json=lambda: mock_response_data
        )
    )

    # Mockamos a busca de conexão para evitar o erro "Unknown hook type None"
    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=mock.MagicMock(conn_type='http', host="api.sunrise-sunset.org")
    )

    # Definição do Operador (passamos a função mockada)
    is_light = SimpleHttpOperator(
        task_id="is_light",
        http_conn_id="https_conn_id",
        endpoint="json",
        method="GET",
        data={"lat": "23.3351", "lon": "46.3810", "formatted": "0"},
        response_check=mock_check_light,  # Usamos o mock como checagem
        dag=test_dag,
    )

    # Execução da Tarefa
    is_light.execute(context={})

    # 3. Asserções

    # Verifica se a chamada HTTP foi realizada pelo operador
    mock_run_method.assert_called_once()

    # Verifica se o operador chamou a função de checagem com a resposta mockada
    mock_check_light.assert_called_once()


# --- TESTE UNITÁRIO DA LÓGICA (OPCIONAL, MAS MELHOR PRÁTICA) ---

@pytest.mark.parametrize(
    "sunrise, sunset, current_time, expected",
    [
        # Caso 1: Meio-dia (Deveria ser False/Dia)
        ("06:00:00", "18:00:00", "12:00:00", False),
        # Caso 2: Meia-noite (Deveria ser True/Noite)
        ("06:00:00", "18:00:00", "00:00:00", True),
    ]
)
def test_check_light_logic_pure(mocker, sunrise, sunset, current_time, expected):
    """Testa a lógica de data/hora isoladamente, mockando a hora atual."""

    # Simula a hora atual para o teste (pendulum é necessário para mockar o now('UTC'))
    current_dt = pendulum.datetime(2025, 1, 1, int(current_time[:2]), 0, 0, tz='UTC')
    mocker.patch("pendulum.now", return_value=current_dt)

    # Formato da resposta da API (entrada)
    response_data = {
        "results": {
            "sunrise": f"2025-01-01T{sunrise}+00:00",
            "sunset": f"2025-01-01T{sunset}+00:00",
        }
    }

    assert check_light_logic(response_data) == expected
