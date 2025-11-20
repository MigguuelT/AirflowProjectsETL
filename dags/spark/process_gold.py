import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(args):
    spark = SparkSession.builder \
        .appName("GoldLayerProcessing") \
        .getOrCreate()

    silver_base = args.silver_base_path
    gold_base = args.gold_base_path
    process_date = args.ds_date

    print(f"Lendo camada Silver em: {silver_base}")

    # Leitura das Tabelas Silver
    try:
        orders = spark.read.parquet(f"{silver_base}/orders")
        order_items = spark.read.parquet(f"{silver_base}/orderitems")
        products = spark.read.parquet(f"{silver_base}/products")
        customers = spark.read.parquet(f"{silver_base}/customers")
    except Exception as e:
        print(f"Erro ao ler tabelas Silver: {e}")
        spark.stop()
        return

    # Criar Views Temporárias para facilitar SQL
    orders.createOrReplaceTempView("orders")
    order_items.createOrReplaceTempView("order_items")
    products.createOrReplaceTempView("products")
    customers.createOrReplaceTempView("customers")

    print("Calculando KPIs...")

    # KPI 1: Número Total de Pedidos (Considerando o historico inteiro da Silver)
    # Se quiser apenas do dia, adicionar WHERE date = ...
    total_orders = spark.sql("""
                             SELECT count(distinct orderid) as total_orders, max(date) as ref_date
                             FROM orders
                             """)

    # KPI 2: Top 5 Pedidos de Maior Valor
    top_5_orders = spark.sql("""
                             SELECT o.orderid, c.firstname, c.lastname, o.totalamount, o.orderdate
                             FROM orders o
                                      JOIN customers c ON o.customerid = c.customerid
                             ORDER BY o.totalamount DESC LIMIT 5
                             """)

    # KPI 3: Top 5 Produtos Mais Vendidos
    top_5_products = spark.sql("""
                               SELECT p.productname, SUM(oi.quantity) as total_sold
                               FROM order_items oi
                                        JOIN products p ON oi.productid = p.productid
                               GROUP BY p.productname
                               ORDER BY total_sold DESC LIMIT 5
                               """)

    # KPI 4: 5 Produtos com Menor Estoque
    low_stock_products = spark.sql("""
                                   SELECT productname, stockquantity
                                   FROM products
                                   ORDER BY stockquantity ASC LIMIT 5
                                   """)

    # Gravação na Camada Gold
    # Salvamos sobrescrevendo a partição do dia (ou pasta específica)

    print("Salvando na camada Gold...")

    base_output = f"{gold_base}/date={process_date}"

    total_orders.write.mode("overwrite").parquet(f"{base_output}/total_orders")
    top_5_orders.write.mode("overwrite").parquet(f"{base_output}/top_5_orders_value")
    top_5_products.write.mode("overwrite").parquet(f"{base_output}/top_5_products_sold")
    low_stock_products.write.mode("overwrite").parquet(f"{base_output}/low_stock_products")

    print(f"Processamento Gold concluído em: {base_output}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver_base_path", required=True)
    parser.add_argument("--gold_base_path", required=True)
    parser.add_argument("--ds_date", required=True)

    args = parser.parse_args()
    main(args)