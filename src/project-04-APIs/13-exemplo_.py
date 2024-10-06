import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, when

# Definir o caminho relativo baseado no diretório de execução (para armazenar Delta Table)
current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(current_dir, '13-data')
delta_dir = os.path.join(data_dir, '13-delta')
src_dir = os.path.join(data_dir, '13-src')

clientes_csv_path = os.path.join(src_dir, '13-src-clientes.csv')

# Configuração da Spark Session com suporte ao Delta Lake
spark = SparkSession.builder \
    .appName("Exemplo13-SCD-Tipo2") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Leitura dos novos dados de clientes (atualizações)
clientes_atualizados_df = spark.read.csv(clientes_csv_path, header=True, inferSchema=True)

# 2. Leitura dos dados existentes da tabela de clientes no Delta Lake (dimensão existente)
try:
    clientes_dim_df = spark.read.format("delta").load(delta_dir)
except Exception as e:
    print(f"Nenhum dado anterior encontrado. Iniciando o processamento com todos os dados. Erro: {e}")
    clientes_dim_df = spark.createDataFrame([], clientes_atualizados_df.schema)

# 3. Marcar os registros existentes como "expirados" (is_current=False) para alterações
clientes_expirados_df = clientes_dim_df \
    .join(clientes_atualizados_df, "cliente_id", "left") \
    .withColumn("is_current", when(clientes_dim_df["nome"] != clientes_atualizados_df["nome"], lit(False)).otherwise(lit(True))) \
    .withColumn("valid_until", when(col("is_current") == False, current_date()).otherwise(col("valid_until")))

# 4. Filtrar os novos registros e criar versões de clientes modificados
clientes_novos_df = clientes_atualizados_df \
    .join(clientes_dim_df, "cliente_id", "left_anti") \
    .withColumn("is_current", lit(True)) \
    .withColumn("valid_from", current_date()) \
    .withColumn("valid_until", lit(None).cast("date"))

# 5. Atualizar a tabela Delta (adicionar novos registros e atualizar expirados)
clientes_atualizados_final_df = clientes_expirados_df.union(clientes_novos_df)

# Escrever a nova versão da tabela de clientes no Delta Lake
clientes_atualizados_final_df.write.format("delta").mode("overwrite").save(delta_dir)

# 6. Leitura da tabela Delta para verificar as alterações aplicadas
print("Leitura dos dados de clientes com SCD Tipo 2:")
delta_df = spark.read.format("delta").load(delta_dir)
delta_df.show(truncate=False)

# Encerrar a Spark Session
spark.stop()
