from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, expr, to_date

# Criar a Spark session
spark = SparkSession.builder \
    .appName("Silver Layer - Carga Incremental") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Diretórios da camada Silver
silver_orders_path = "/workspace/14-data/silver/orders"
silver_inventory_path = "/workspace/14-data/silver/inventory_movements"

# Função para otimizar o merge dos dados incrementais com o Delta Lake
def merge_incremental_data(delta_table_path, new_data_df, key_column):
    if DeltaTable.isDeltaTable(spark, delta_table_path):
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        delta_table.alias("target").merge(
            new_data_df.alias("source"),
            f"target.{key_column} = source.{key_column}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        # Se a tabela Delta não existir, cria uma nova
        new_data_df.write.format("delta").mode("overwrite").save(delta_table_path)

# 1. Leitura dos dados incrementais da camada Bronze (simulação de carga incremental)
orders_incremental_df = spark.read.csv("/workspace/14-data/bronze/orders_incremental.csv", header=True, inferSchema=True)
inventory_incremental_df = spark.read.csv("/workspace/14-data/bronze/inventory_movements_incremental.csv", header=True, inferSchema=True)

# 2. Aplicar transformações na camada Silver
# Remover pedidos com status 'cancelado' e converter a coluna de data
orders_silver_df = orders_incremental_df.filter(col("status") != "cancelado") \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# Normalizar as movimentações de estoque e calcular o saldo
inventory_silver_df = inventory_incremental_df \
    .withColumn("movement_type", expr("CASE WHEN quantity > 0 THEN 'IN' ELSE 'OUT' END"))

# Otimizar as consultas com particionamento por data na camada Silver
orders_silver_df.write.format("delta").mode("append").partitionBy("order_date").save(silver_orders_path)
inventory_silver_df.write.format("delta").mode("append").partitionBy("movement_date").save(silver_inventory_path)

# 3. Carga incremental otimizada com Delta Lake (usando merge)
merge_incremental_data(silver_orders_path, orders_silver_df, "order_id")
merge_incremental_data(silver_inventory_path, inventory_silver_df, "movement_id")

# Verificar os dados carregados na camada Silver
orders_silver_df.show(truncate=False)
inventory_silver_df.show(truncate=False)

# Encerrar a Spark session
spark.stop()
