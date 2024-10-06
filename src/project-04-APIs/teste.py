import os
from pyspark.sql import SparkSession
from delta import *

# Definir o caminho relativo baseado no diretório de execução
current_dir = os.path.dirname(os.path.realpath(__file__))  
delta_dir = os.path.join(current_dir, 'delta-table')  # Substitua 'delta-table' pelo nome desejado

# Criar uma Spark Session com suporte ao Delta Lake
spark = SparkSession.builder \
    .appName("DeltaTest") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Criar uma tabela Delta simples
data = [(1, "A"), (2, "B"), (3, "C")]
df = spark.createDataFrame(data, ["id", "value"])

# Salvar em formato Delta no caminho relativo
df.write.format("delta").mode("overwrite").save(delta_dir)

# Ler a tabela Delta do caminho relativo
delta_df = spark.read.format("delta").load(delta_dir)
delta_df.show()

# Encerrar a Spark Session
spark.stop()
