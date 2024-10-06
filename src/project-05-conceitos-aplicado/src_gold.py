from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Criar a Spark session
spark = SparkSession.builder \
    .appName("Gold Layer - Incremental Load and Optimization") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Caminho da camada Silver e Gold
silver_customers_path = "/workspace/14-data/silver/customers"
silver_orders_path = "/workspace/14-data/silver/orders"
silver_inventory_path = "/workspace/14-data/silver/inventory_movements"
gold_sales_report_path = "/workspace/14-data/gold/sales_report"

# 1. Leitura dos dados da camada Silver
customers_silver_df = spark.read.format("delta").load(silver_customers_path)
orders_silver_df = spark.read.format("delta").load(silver_orders_path)
inventory_silver_df = spark.read.format("delta").load(silver_inventory_path)

# 2. Unir os dados para criar o relatório de vendas na camada Gold
# Juntando clientes e pedidos
sales_df = orders_silver_df.join(customers_silver_df, "customer_id") \
    .select("order_id", "customer_id", "nome", "total_amount", "order_date", "cidade", "estado")

# Juntando a movimentação de estoque para monitorar o status dos produtos
final_report_df = sales_df.join(inventory_silver_df, "order_id", "left_outer") \
    .select(
        "order_id", "customer_id", "nome", "cidade", "estado", 
        "total_amount", "order_date", "movement_type", "quantity"
    )

# Exibir o relatório final
print("Relatório de Vendas na Camada Gold:")
final_report_df.show(truncate=False)

# 3. Carga Incremental na Camada Gold (com Z-Ordering)
if DeltaTable.isDeltaTable(spark, gold_sales_report_path):
    # Atualizando os dados na camada Gold
    final_report_df.write.format("delta").mode("overwrite").save(gold_sales_report_path)
else:
    # Primeira carga
    final_report_df.write.format("delta").mode("overwrite").save(gold_sales_report_path)

# 4. Otimização de Particionamento e Z-Ordering
# Particionamento por estado para melhorar a eficiência das consultas
final_report_df.write.format("delta").partitionBy("estado").mode("overwrite").save(gold_sales_report_path)

# Aplicar Z-Ordering para otimização adicional (no Delta Lake)
delta_table_gold = DeltaTable.forPath(spark, gold_sales_report_path)
delta_table_gold.optimize().executeZOrderBy("order_date")

# Exibir o relatório otimizado
print("Relatório de Vendas (Otimizado):")
optimized_df = spark.read.format("delta").load(gold_sales_report_path)
optimized_df.show(truncate=False)

# Encerrar a Spark session
spark.stop()
