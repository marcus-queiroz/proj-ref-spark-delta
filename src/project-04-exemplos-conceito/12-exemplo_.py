import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, max

# Definir o caminho relativo baseado no diretório de execução (para armazenar Delta Table)
current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(current_dir, '12-data')
delta_dir = os.path.join(data_dir, '12-delta')
src_dir = os.path.join(data_dir, '12-src')

transacoes_csv_path = os.path.join(src_dir, '12-src-transacoes.csv')

# Configuração da Spark Session com suporte ao Delta Lake
spark = SparkSession.builder \
    .appName("Exemplo12-Carregamento-Incremental") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Leitura dos novos dados de transações (incrementais)
transacoes_df = spark.read.csv(transacoes_csv_path, header=True, inferSchema=True)

# 2. Detectar o último registro processado na tabela Delta (se existir)
try:
    delta_df = spark.read.format("delta").load(delta_dir)
    ultima_data_processada = delta_df.agg(max(col("data"))).collect()[0][0]
except Exception as e:
    print(f"Nenhum dado anterior encontrado. Iniciando o processamento com todos os dados. Erro: {e}")
    ultima_data_processada = None

# 3. Filtrar as transações incrementais (novas ou alteradas)
if ultima_data_processada:
    transacoes_incrementais = transacoes_df.filter(
        expr(f"to_date(data, 'yyyy-MM-dd') > to_date('{ultima_data_processada}', 'yyyy-MM-dd')")
    )
else:
    transacoes_incrementais = transacoes_df

print("Transações incrementais (novas ou alteradas):")
transacoes_incrementais.show(truncate=False)

# 4. Escrever as novas transações no Delta Lake
transacoes_incrementais.write.format("delta").mode("append").save(delta_dir)

# 5. Leitura da tabela Delta para verificar as transações processadas
print("Leitura dos dados processados no Delta Lake:")
delta_df = spark.read.format("delta").load(delta_dir)
delta_df.show(truncate=False)

# Encerrar a Spark Session
spark.stop()
