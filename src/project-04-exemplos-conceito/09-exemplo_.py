import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, when

# Definir o caminho relativo baseado no diretório de execução (para armazenar Delta Table)
current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(current_dir, '09-data')
delta_dir = os.path.join(data_dir, '09-delta')

# Configuração da Spark Session com suporte ao Delta Lake
spark = SparkSession.builder \
    .appName("Exemplo9-SCD-Tipo2-Particionado") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Leitura dos dados de clientes (primeira carga)
clientes_novos = [
    (1, "Rich", "São Paulo", "2024-10-01"),
    (2, "Carlos", "Belo Horizonte", "2024-10-01"),
    (3, "Julia", "Brasília", "2024-10-01"),
    (4, "Ana", "Rio de Janeiro", "2024-10-01")
]

schema = ["cliente_id", "nome", "cidade", "data_cadastro"]
clientes_novos_df = spark.createDataFrame(clientes_novos, schema)

# Adicionar colunas necessárias para SCD Tipo 2
clientes_novos_df = clientes_novos_df.withColumn("ativo", lit(True)) \
    .withColumn("data_inicio", current_date()) \
    .withColumn("data_fim", lit(None).cast("date"))

# Escrever os dados na tabela Delta com particionamento pela coluna "cidade"
clientes_novos_df.write.format("delta").partitionBy("cidade").mode("overwrite").save(delta_dir)

# 2. Simular mudanças nos dados de clientes
clientes_atualizados = [
    (1, "Rich", "Campinas", "2024-11-01"),  # Rich mudou de cidade
    (2, "Carlos", "Belo Horizonte", "2024-11-01"),  # Carlos continua igual
    (3, "Julia", "Curitiba", "2024-11-01"),  # Julia mudou de cidade
    (5, "Marcos", "São Paulo", "2024-11-01")  # Novo cliente
]

clientes_atualizados_df = spark.createDataFrame(clientes_atualizados, schema)

# 3. Leitura dos dados históricos do Delta Lake
clientes_delta_df = spark.read.format("delta").load(delta_dir)

# 4. Identificação de mudanças nos dados (SCD Tipo 2)
condicao_mudanca = (clientes_atualizados_df.cliente_id == clientes_delta_df.cliente_id) & \
                   (clientes_atualizados_df.cidade != clientes_delta_df.cidade) & \
                   (clientes_delta_df.ativo == True)

# Atualizar registros antigos como inativos
clientes_historico_atualizado_df = clientes_delta_df.join(clientes_atualizados_df, "cliente_id", "left") \
    .withColumn("ativo", when(condicao_mudanca, lit(False)).otherwise(clientes_delta_df.ativo)) \
    .withColumn("data_fim", when(condicao_mudanca, current_date()).otherwise(clientes_delta_df.data_fim))

# 5. Inserir novos registros ou registros alterados com nova cidade
clientes_novos_atualizados_df = clientes_atualizados_df.join(clientes_delta_df, "cliente_id", "leftanti") \
    .withColumn("ativo", lit(True)) \
    .withColumn("data_inicio", current_date()) \
    .withColumn("data_fim", lit(None).cast("date"))

# 6. Combinar registros antigos e novos
clientes_final_df = clientes_historico_atualizado_df.unionByName(clientes_novos_atualizados_df)

# 7. Escrever os dados finalizados no Delta Lake com particionamento
clientes_final_df.write.format("delta").partitionBy("cidade").mode("overwrite").save(delta_dir)

# 8. Exibir o resultado final
print("Histórico de clientes com SCD Tipo 2 e Particionamento:")
clientes_result_df = spark.read.format("delta").load(delta_dir)
clientes_result_df.show(truncate=False)

# Encerrar a Spark Session
spark.stop()
