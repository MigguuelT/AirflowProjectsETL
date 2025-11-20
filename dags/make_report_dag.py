import io
import base64
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import os
import glob
import re  # Necessário para manipular texto, se o usuário quiser TXT

# 🛑 CORREÇÃO CRÍTICA: Usar backend Agg para evitar erros de GUI/Matplotlib
import matplotlib

matplotlib.use('Agg')

# --- CONFIGURAÇÕES ---
# Ajuste para o caminho local
LOCAL_BASE_PATH = "/Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3/data-lake/gold/sales"
# Pasta onde os arquivos HTML/TXT serão salvos (Ex: /Users/.../Airflow_2.9.3/reports)
REPORT_OUTPUT_DIR = "/Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3/reports"

# Email de destino
EMAIL_TO = "migueltori78@gmail.com"


# --- FUNÇÕES AUXILIARES ---

def create_plot_image(df, x_col, y_col, title, kind='bar', color='skyblue'):
    """Gera um gráfico e retorna a string Base64 para HTML."""
    plt.figure(figsize=(8, 4))

    if kind == 'bar':
        plt.bar(df[x_col], df[y_col], color=color)
    elif kind == 'barh':
        df = df.sort_values(by=y_col, ascending=True)
        plt.barh(df[x_col], df[y_col], color=color)

    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()

    img_base64 = base64.b64encode(buf.read()).decode('utf-8')
    return f'<img src="data:image/png;base64,{img_base64}" alt="{title}">'


def safe_read_parquet_dir(base_dir, metric_folder):
    """Lê um arquivo Parquet (part-xxx.parquet) de um diretório de métrica."""
    full_path = os.path.join(base_dir, metric_folder)
    files = glob.glob(os.path.join(full_path, "*.parquet"))

    if not files:
        print(f"AVISO: Arquivo Parquet não encontrado em {full_path}. Retornando DataFrame vazio.")
        if metric_folder == 'total_orders':
            return pd.DataFrame({'total_orders': [0]})
        if metric_folder == 'top_5_orders_value':
            return pd.DataFrame(columns=['orderid', 'firstname', 'lastname', 'totalamount', 'orderdate'])
        if metric_folder == 'top_5_products_sold':
            return pd.DataFrame(columns=['productname', 'total_sold'])
        if metric_folder == 'low_stock_products':
            return pd.DataFrame(columns=['productname', 'stockquantity'])
        return pd.DataFrame()

    return pd.read_parquet(files[0])


def generate_html_report(**context):
    """Lê dados Gold, gera gráficos e cria o HTML (retornado via XCom)."""
    ds = context['ds']
    partition_path = os.path.join(LOCAL_BASE_PATH, f"date={ds}")

    print(f"Lendo dados Gold de: {partition_path}")

    # Leitura e Geração de Gráficos (Lógica Omitida para concisão, assume-se que retorna HTML)
    # [Restante da lógica de leitura e geração de HTML deve ser mantida aqui]

    df_total = safe_read_parquet_dir(partition_path, 'total_orders')
    df_top_orders = safe_read_parquet_dir(partition_path, 'top_5_orders_value')
    df_top_products = safe_read_parquet_dir(partition_path, 'top_5_products_sold')
    df_low_stock = safe_read_parquet_dir(partition_path, 'low_stock_products')

    total_orders_val = df_total['total_orders'].iloc[0] if 'total_orders' in df_total.columns else 0

    chart_products = ""
    if not df_top_products.empty:
        chart_products = create_plot_image(
            df_top_products.head(5), 'productname', 'total_sold',
            'Top 5 Produtos Mais Vendidos', color='#4CAF50'
        )

    chart_stock = ""
    if not df_low_stock.empty:
        chart_stock = create_plot_image(
            df_low_stock.head(5), 'productname', 'stockquantity',
            'Top 5 Produtos com Menor Estoque', kind='barh', color='#F44336'
        )

    # Simulação da Construção do HTML (Para simplificação visual)
    html_content = f"""
    <html>
    ...
    <body>
        <h1>Relatório Diário de Vendas e Estoque</h1>
        <div class="kpi-box"><h3>Total de Pedidos Processados</h3><p>{total_orders_val}</p></div>
        <h2>🏆 Top 5 Pedidos</h2>{df_top_orders.to_html(index=False, border=0)}
        {chart_products}
        <h2>⚠️ Alerta de Estoque Baixo</h2>
        {chart_stock}
    </body>
    </html>
    """

    return html_content


def save_report_to_local_file(ti, ds, report_output_dir):
    """
    Puxa o HTML do XCom e salva no disco como um arquivo HTML para visualização.
    """
    html_content = ti.xcom_pull(task_ids='generate_html_content')

    if not html_content:
        print("ERRO: Conteúdo HTML não encontrado no XCom.")
        return

    # 1. Salvar como .html (Melhor para visualização)
    os.makedirs(report_output_dir, exist_ok=True)
    html_file_path = os.path.join(report_output_dir, f"sales_report_{ds}.html")

    with open(html_file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"✅ Relatório HTML salvo localmente para visualização em: {html_file_path}")

    # 2. Salvar como .txt (Se estritamente necessário, remove HTML tags)
    # Apenas se o usuário realmente quiser, pois o formato TXT perde a formatação e gráficos.
    # text_content = re.sub(r'<[^>]+>', '', html_content)
    # text_file_path = os.path.join(report_output_dir, f"sales_report_{ds}.txt")
    # with open(text_file_path, "w", encoding="utf-8") as f:
    #     f.write(text_content)
    # print(f"✅ Relatório TXT salvo em: {text_file_path}")


with DAG(
        dag_id='make_report_dag',
        start_date=days_ago(1),
        schedule_interval='@daily',
        catchup=False,
        tags=['report', 'email', 'gold']
) as dag:
    # 1. Gera o HTML e Gráficos
    generate_content_task = PythonOperator(
        task_id='generate_html_content',
        python_callable=generate_html_report,
        provide_context=True
    )

    # 2. Salva o HTML no disco local para visualização
    save_report_task = PythonOperator(
        task_id='save_report_to_disk',
        python_callable=save_report_to_local_file,
        op_kwargs={
            'ds': '{{ ds }}',
            'report_output_dir': REPORT_OUTPUT_DIR
        }
    )

    # 3. Tenta Enviar o Email (Ainda pode falhar, mas o relatório local está salvo)
    send_email_task = EmailOperator(
        task_id='send_email_report',
        to=EMAIL_TO,
        subject='Relatório Diário: Vendas, Produtos e Estoque',
        html_content="{{ task_instance.xcom_pull(task_ids='generate_html_content') }}",
        conn_id='smtp_etl'
    )

    generate_content_task >> save_report_task
    save_report_task >> send_email_task