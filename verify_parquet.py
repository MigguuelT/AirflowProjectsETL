from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------
# IMPORTANTE: ATUALIZE ESTE CAMINHO ANTES DE RODAR
# Altere a data '2025-11-18' para a data da última execução da sua DAG.
# --------------------------------------------------------------------------------
PARQUET_PATH = "file:///Users/migueltorikachvili/PycharmProjects/Airflow_2.9.3/data-lake/bronze/sales/customers/date=2025-11-18"

def main():
    """Lê o arquivo Parquet e exibe o esquema e os dados."""
    spark = SparkSession.builder \
        .appName("ParquetReader") \
        .master("local[*]") \
        .getOrCreate()
    
    print(f"\nTentando ler Parquet em: {PARQUET_PATH}\n")
    
    try:
        # Lê todos os arquivos Parquet na pasta particionada
        df = spark.read.parquet(PARQUET_PATH)
        
        print("✅ Esquema do DataFrame (Colunas e Tipos):")
        df.printSchema()
        
        print("\n✅ Primeiras 5 Linhas dos Dados (Confirmação ETL):")
        df.show(5)
        
    except Exception as e:
        print(f"\n❌ ERRO ao ler o Parquet: {e}")
        print("Verifique se o caminho PARQUET_PATH e a data estão corretos.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()