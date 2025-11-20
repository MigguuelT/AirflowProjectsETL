import pandas as pd

AIRFLOW_HOME= '/Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3'

def le_arquivo():
    df = pd.read_csv(f'{AIRFLOW_HOME}/data/novo.csv', header=None)
    return df

def mascara_senha(df):
    df[2] = '****'
    return df

def salva_arquivo(df):
    df.to_csv(f'{AIRFLOW_HOME}/data/novo_final.csv', index=False)

