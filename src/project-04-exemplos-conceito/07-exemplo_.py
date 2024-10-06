import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr

# Definir o caminho relativo baseado no diretório de execução (para armazenar Delta Table)
current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(current_dir, '07-data')
delta_dir = os.path.join(data_dir, '07-delta')
dst_dir = os.path.join(data_dir, '07-dst')

transacoes_csv_path = os.path.join(data_dir, '07-src-transacoes.csv')
clientes_json_path = os.path.join(data_dir, '07-src-clientes.json')

# Criar uma Spark Session com suporte ao Delta Lake (sem Hadoop)
spark = SparkSession.builder \
    .appName("Exemplo7-CargaIncremental-Sem-Hadoop") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[*]") \
    .getOrCreate()

# 1. Leitura dos arquivos CSV e JSON com caminho relativo
transacoes_df = spark.read.csv(transacoes_csv_path, header=True, inferSchema=True)
clientes_df = spark.read.json(clientes_json_path)

# Renomear a coluna 'id' em transacoes_df e clientes_df para evitar ambiguidade
transacoes_df = transacoes_df.withColumnRenamed("id", "transacao_id")
clientes_df = clientes_df.withColumnRenamed("id", "cliente_id")

# 2. Transformação: Conversão de data e filtragem incremental
ultima_data_processada = "2024-03-01"  
transacoes_filtradas = transacoes_df.filter(
    to_date(col("data"), "yyyy-MM-dd") > to_date(expr(f"'{ultima_data_processada}'"), "yyyy-MM-dd")
)

# Exibir as novas transações (incrementais)
print("Transações incrementais (novas desde a última execução):")
transacoes_filtradas.show(truncate=False)

# 3. Juntar com os clientes para obter mais informações
transacoes_clientes_df = transacoes_filtradas.join(clientes_df, transacoes_filtradas.cliente_id == clientes_df.cliente_id, "inner")

# Exibir o resultado da junção
print("Transações incrementais com dados de clientes:")
transacoes_clientes_df.select(
    col("transacao_id"),
    col("nome").alias("cliente_nome"),
    col("valor"),
    col("categoria"),
    col("cidade"),
    col("data")
).show(truncate=False)

# 4. Escrever os resultados incrementais no Delta Lake (sem Hadoop, usando sistema de arquivos local)
transacoes_clientes_df.write.format("delta").mode("append").save(delta_dir)

# 5. Leitura do Delta Lake para validar a persistência incremental
print("Leitura dos dados salvos no Delta Lake:")
delta_df = spark.read.format("delta").load(delta_dir)
delta_df.show(truncate=False)

# Encerrar a Spark Session
spark.stop()
