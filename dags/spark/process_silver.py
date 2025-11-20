import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType


def process_data(spark: SparkSession, table_name: str, bronze_path: str, silver_path: str, is_incremental: bool,
                 primary_key: str = None):
    """
    Processa dados da camada Bronze para a Silver, aplicando deduplicação para Full Load
    ou simplesmente anexando para Incremental Load.
    """
    print(f"Iniciando processamento para a tabela: {table_name}")
    print(f"  Modo Incremental: {is_incremental}")
    print(f"  Caminho Bronze: {bronze_path}")
    print(f"  Caminho Silver: {silver_path}")

    try:
        # 1. Leitura dos dados da camada Bronze
        df = spark.read.parquet(bronze_path)

        # 1.1. Conversão de tipos para estabilizar (necessário após a fase Bronze)
        if table_name == 'orders':
            df = df.withColumn("totalamount", F.col("totalamount").cast(DoubleType()))
            df = df.withColumn("orderid", F.col("orderid").cast(IntegerType()))
            df = df.withColumn("customerid", F.col("customerid").cast(IntegerType()))
        if table_name == 'orderitems':
            df = df.withColumn("itemprice", F.col("itemprice").cast(DoubleType()))

        # Verificação crucial: se o DataFrame estiver vazio, logamos e encerramos.
        if df.count() == 0:
            print(f"AVISO: Nenhum dado encontrado para processar na Bronze em {bronze_path}. Encerrando a escrita.")
            return

        print(f"Dados lidos: {df.count()} registros.")

        # 2. Lógica de Processamento
        if is_incremental:
            # Lógica para tabelas incrementais (ex: orders)

            # 2.1. Validar a coluna 'date'
            if 'date' not in df.columns:
                print(
                    "ERRO CRÍTICO: A coluna 'date' não foi encontrada. O particionamento da Bronze deve ser 'date=YYYY-MM-DD'.")
                return

            # 2.2. Escreve com modo 'append' e particionamento
            df.write \
                .mode("append") \
                .partitionBy("date") \
                .parquet(silver_path)

            print(f"Incremental Load concluído para {table_name}. Anexados {df.count()} registros à Silver.")

        else:
            # Lógica para tabelas Full Load (ex: customers, products, orderitems) com deduplicação
            if not primary_key:
                print("ERRO: Tabela Full Load requer 'primary_key'. Encerrando.")
                return

            # 2.1. Cria uma coluna 'rank' para identificar o registro mais recente
            # ✅ CORREÇÃO: Usar a coluna 'date' (data da carga) como o campo de ordenação/versão.
            window_spec = Window.partitionBy(primary_key).orderBy(F.col("date").desc())

            df_ranked = df.withColumn("rank", F.rank().over(window_spec))

            # 2.2. Filtra o registro mais recente para cada chave primária
            df_deduplicated = df_ranked.filter(F.col("rank") == 1).drop("rank")

            # 2.3. Escreve com modo 'overwrite'
            df_deduplicated.write \
                .mode("overwrite") \
                .parquet(silver_path)

            print(
                f"Full Load concluído para {table_name}. Escritos {df_deduplicated.count()} registros deduplicados na Silver.")

    except Exception as e:
        print(f"Erro ao processar a tabela {table_name}: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processa dados da camada Bronze para Silver.")
    parser.add_argument("--table_name", required=True, help="Nome da tabela (ex: customers, orders).")
    parser.add_argument("--bronze_path", required=True,
                        help="Caminho de leitura da Bronze (ex: /path/to/bronze/table).")
    parser.add_argument("--silver_path", required=True,
                        help="Caminho de escrita na Silver (ex: /path/to/silver/table).")
    parser.add_argument("--is_incremental", action="store_true",
                        help="Se presente, usa modo de processamento incremental (append).")
    parser.add_argument("--primary_key", help="Chave primária para deduplicação (apenas para Full Load).", default=None)

    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"SilverLayer_{args.table_name}").getOrCreate()

    process_data(
        spark,
        args.table_name,
        args.bronze_path,
        args.silver_path,
        args.is_incremental,
        args.primary_key
    )

    spark.stop()