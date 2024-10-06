from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# Criar a Spark session
spark = SparkSession.builder \
    .appName("CDC Simulation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Diretórios dos dados
bronze_path = "/workspace/14-data/bronze/orders"
silver_path = "/workspace/14-data/silver/orders"

# Função para realizar Merge (CDC)
def merge_cdc(delta_table, updates_df, key_column):
    delta_table.alias("target").merge(
        updates_df.alias("source"),
        f"target.{key_column} = source.{key_column}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Leitura dos pedidos existentes da camada Bronze (CDC Incremental)
if DeltaTable.isDeltaTable(spark, bronze_path):
    orders_bronze = DeltaTable.forPath(spark, bronze_path)
    orders_bronze_df = orders_bronze.toDF()
else:
    # Caso não exista, criar a primeira tabela Delta na Bronze
    orders_bronze_df = spark.read.csv("/workspace/14-data/new_orders.csv", header=True, inferSchema=True)

# Simular novos pedidos e atualizações em pedidos existentes
new_orders = [
    (5, 3, "2024-09-01", 125.50, "completed"),
    (6, 4, "2024-09-02", 200.00, "pending"),  # Novo pedido
    (1, 2, "2024-09-01", 99.99, "completed")  # Atualização de pedido
]

new_orders_df = spark.createDataFrame(new_orders, schema=orders_bronze_df.schema)

# Realizar o merge para aplicar as mudanças incrementais
if DeltaTable.isDeltaTable(spark, bronze_path):
    merge_cdc(orders_bronze, new_orders_df, "order_id")
else:
    # Escrever a nova tabela Delta caso seja a primeira carga
    new_orders_df.write.format("delta").mode("overwrite").save(bronze_path)

# Aplicar o CDC na camada Silver (dados normalizados e atualizados)
if DeltaTable.isDeltaTable(spark, silver_path):
    orders_silver = DeltaTable.forPath(spark, silver_path)
    merge_cdc(orders_silver, new_orders_df, "order_id")
else:
    # Criar tabela Delta na Silver com as novas atualizações
    new_orders_df.write.format("delta").mode("overwrite").save(silver_path)

# Verificar os dados na camada Silver após o CDC
silver_df = spark.read.format("delta").load(silver_path)
silver_df.show(truncate=False)

# Encerrar a Spark session
spark.stop()
