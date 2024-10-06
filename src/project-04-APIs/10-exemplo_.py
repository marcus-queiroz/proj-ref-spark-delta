import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr, current_timestamp

# Definir o caminho relativo baseado no diretório de execução (para armazenar Delta Table)
current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(current_dir, '10-data')
delta_dir = os.path.join(data_dir, '10-delta')
snapshot_dir = os.path.join(data_dir, '10-snapshots')

# Configuração da Spark Session com suporte ao Delta Lake
spark = SparkSession.builder \
    .appName("Exemplo10-Snapshot-Delta") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Leitura dos dados de transações (primeira carga)
transacoes = [
    (1, 1, 500.0, "2024-01-01", "Alimentos"),
    (2, 2, 300.0, "2024-01-02", "Eletrônicos"),
    (3, 1, 200.0, "2024-01-03", "Alimentos"),
    (4, 3, 1500.0, "2024-01-04", "Viagem")
]

schema = ["transacao_id", "cliente_id", "valor", "data", "categoria"]
transacoes_df = spark.createDataFrame(transacoes, schema)

# Escrever os dados de transações no Delta Lake
transacoes_df.write.format("delta").mode("overwrite").save(delta_dir)

# Função para capturar o estado completo dos dados (Snapshot)
def criar_snapshot(spark_session, delta_table_path, snapshot_path, snapshot_version):
    # Leitura dos dados do Delta Lake
    delta_df = spark_session.read.format("delta").load(delta_table_path)
    
    # Capturar o estado atual dos dados e salvar como snapshot
    delta_df.withColumn("snapshot_timestamp", current_timestamp()) \
            .write.format("delta").mode("overwrite").save(os.path.join(snapshot_path, f"snapshot_v{snapshot_version}"))

# 2. Simular novas transações (carga incremental)
novas_transacoes = [
    (5, 2, 700.0, "2024-02-01", "Eletrônicos"),
    (6, 4, 1200.0, "2024-02-02", "Viagem"),
    (7, 1, 600.0, "2024-02-03", "Alimentos"),
    (8, 3, 900.0, "2024-02-04", "Eletrônicos")
]

novas_transacoes_df = spark.createDataFrame(novas_transacoes, schema)

# Escrever as novas transações no Delta Lake (incremental)
novas_transacoes_df.write.format("delta").mode("append").save(delta_dir)

# 3. Capturar o primeiro snapshot após a primeira carga
criar_snapshot(spark, delta_dir, snapshot_dir, 1)

# 4. Mais uma rodada de transações
transacoes_fevereiro = [
    (9, 2, 400.0, "2024-03-01", "Alimentos"),
    (10, 4, 500.0, "2024-03-02", "Eletrônicos")
]

transacoes_fevereiro_df = spark.createDataFrame(transacoes_fevereiro, schema)

# Escrever as transações de fevereiro no Delta Lake
transacoes_fevereiro_df.write.format("delta").mode("append").save(delta_dir)

# 5. Capturar o segundo snapshot após a carga de fevereiro
criar_snapshot(spark, delta_dir, snapshot_dir, 2)

# 6. Revisão das versões de snapshots
print("Snapshot versão 1:")
snapshot_v1_df = spark.read.format("delta").load(os.path.join(snapshot_dir, "snapshot_v1"))
snapshot_v1_df.show(truncate=False)

print("Snapshot versão 2:")
snapshot_v2_df = spark.read.format("delta").load(os.path.join(snapshot_dir, "snapshot_v2"))
snapshot_v2_df.show(truncate=False)

# 7. Leitura da tabela Delta com as transações
print("Transações atuais no Delta Lake:")
delta_df = spark.read.format("delta").load(delta_dir)
delta_df.show(truncate=False)

# Encerrar a Spark Session
spark.stop()
