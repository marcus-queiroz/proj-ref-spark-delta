from pyspark.sql import SparkSession

# Criação da Spark Session com Delta Lake
spark = SparkSession.builder \
    .appName("Camada Bronze - Ingestão de Dados Brutos") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Diretórios de origem e destino para os arquivos
bronze_data_dir = "14-data/bronze"

# Leitura dos dados brutos (arquivos CSV e JSON)
orders_df = spark.read.option("header", True).csv("14-data/bronze/orders.csv")
customers_df = spark.read.option("multiline", True).json("14-data/bronze/customers.json")
inventory_movements_df = spark.read.option("header", True).csv("14-data/bronze/inventory_movements.csv")

# Escrita dos dados na camada Bronze (em formato Delta)
orders_df.write.format("delta").mode("overwrite").save(f"{bronze_data_dir}/orders")
customers_df.write.format("delta").mode("overwrite").save(f"{bronze_data_dir}/customers")
inventory_movements_df.write.format("delta").mode("overwrite").save(f"{bronze_data_dir}/inventory_movements")

spark.stop()
