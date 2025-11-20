import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DoubleType, DateType


def main(args):
    """
    Conecta-se ao SQLite via JDBC, carrega os dados, aplica filtro (se for orders)
    e salva o resultado no caminho de saída.
    """
    spark = SparkSession.builder \
        .appName("SQLtoParquet") \
        .getOrCreate()

    table_name = ''
    if args.query:
        # A consulta SQL é usada como uma subconsulta.
        table_name = f'({args.query}) AS tmp'
    else:
        table_name = args.table

    # Configuração JDBC para SQLite
    sqlite_url = f'jdbc:sqlite:{args.database}'
    properties = {
        'driver': 'org.sqlite.JDBC'
    }

    output_path = args.hdfs_path

    print(f"Lendo dados de: jdbc:sqlite:{args.database} com query: {table_name}")
    print(f"Escrevendo em path: {output_path} no modo: {args.write_mode}")

    # Leitura dos dados (Sem CASTs no SQL, confiando na inferência dos tipos corrigidos do BD)
    try:
        df_reader = spark.read.format("jdbc") \
            .option("url", sqlite_url) \
            .option("dbtable", table_name) \
            .options(**properties)

        df = df_reader.load()

    except Exception as e:
        print(f"Erro ao ler do SQLite: {e}")
        spark.stop()
        return

    # Contagem inicial de linhas
    row_count = df.count()
    print(f"✅ Sucesso na leitura da tabela '{table_name}'. Linhas encontradas: {row_count}")

    if row_count == 0:
        print("Aviso: Nenhuma linha encontrada. O Spark não criará a pasta de destino.")
        spark.stop()
        return

    # =================================================================
    # WORKAROUND: FILTRAGEM E CONVERSÃO DE TIPOS NO PYSPARK (ORDERS)
    # A conversão de tipos é feita aqui, pois a inferência direta falhou.
    # =================================================================
    if args.table == 'orders':

        # Converte tipos lidos por inferência para os tipos ideais:
        df = df.withColumn("totalamount", F.col("totalamount").cast(DoubleType()))
        df = df.withColumn("orderid", F.col("orderid").cast(IntegerType()))
        df = df.withColumn("customerid", F.col("customerid").cast(IntegerType()))

        # Aplica o filtro incremental no PySpark.
        # A coluna orderdate (que é TEXT no BD) deve ser comparada com a ds_date.
        df = df.filter(F.col("orderdate") == args.ds_date)

        # Reavalia a contagem após o filtro.
        row_count_filtered = df.count()
        if row_count_filtered == 0:
            print(
                f"Aviso: Nenhuma linha encontrada para a data {args.ds_date} após o filtro de data. O Spark não criará a pasta de destino.")
            spark.stop()
            return

        print(f"Linhas filtradas para {args.ds_date}: {row_count_filtered}")

    # Se for outra tabela, apenas converte os tipos corrigidos (REAL -> Double)
    if args.table == 'orderitems':
        df = df.withColumn("itemprice", F.col("itemprice").cast(DoubleType()))

    # Adiciona a coluna 'date' (para partição)
    df = df.withColumn("date", F.lit(args.ds_date).cast("date"))

    # Escrita no destino
    df.write.mode(args.write_mode).parquet(output_path)

    print("Dados carregados com sucesso no disco local.")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL script to read sqlite table and save it as Parquet HDFS')
    parser.add_argument("--database", required=True, help="Path to the SQLite .db file.")
    parser.add_argument("--table", required=True, help="Table name in SQLite (used if no query is provided).")
    parser.add_argument("--query", required=True, help="SQL query to execute for data extraction.")
    parser.add_argument("--hdfs_path", required=True,
                        help="Path for output (must include scheme like file:// or hdfs://).")
    parser.add_argument("--ds_date", required=True, help="Airflow execution date (ds) for partitioning.")
    parser.add_argument("--write_mode", required=True, help="Spark write mode (overwrite, append).")
    args = parser.parse_args()
    main(args)