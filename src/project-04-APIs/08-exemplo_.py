import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, when, to_date

# Definir o caminho relativo baseado no diretório de execução (para armazenar Delta Table)
current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(current_dir, '08-data')
delta_dir = os.path.join(data_dir, '08-delta')

# Configuração da Spark Session com suporte ao Delta Lake
spark = SparkSession.builder \
    .appName("Exemplo8-SCD-Tipo2") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Leitura dos dados de clientes atuais (data atual)
clientes_novos = [
    (1, "Rich", "São Paulo", "2024-10-01"),
    (2, "Carlos", "Belo Horizonte", "2024-10-01"),
    (3, "Julia", "Brasília", "2024-10-01"),
    (4, "Ana", "Rio de Janeiro", "2024-10-01")
]

schema = ["cliente_id", "nome", "cidade", "data_cadastro"]
clientes_novos_df = spark.createDataFrame(clientes_novos, schema)

# Escrever os dados iniciais na tabela Delta com a flag de SCD Tipo 2 (flag `ativo`)
clientes_novos_df = clientes_novos_df.withColumn("ativo", lit(True)) \
    .withColumn("data_inicio", current_date()) \
    .withColumn("data_fim", lit(None).cast("date"))

clientes_novos_df.write.format("delta").mode("overwrite").save(delta_dir)

# 2. Simular mudanças nos clientes
clientes_atualizados = [
    (1, "Rich", "Campinas", "2024-11-01"),  # Rich mudou de cidade
    (2, "Carlos", "Belo Horizonte", "2024-11-01"),  # Carlos continua igual
    (3, "Julia", "Curitiba", "2024-11-01"),  # Julia mudou de cidade
    (5, "Marcos", "São Paulo", "2024-11-01")  # Novo cliente
]

clientes_atualizados_df = spark.createDataFrame(clientes_atualizados, schema)

# 3. Leitura dos dados históricos (anteriores) do Delta Lake
clientes_delta_df = spark.read.format("delta").load(delta_dir)

# 4. Identificar mudanças: Comparar os dados novos com os históricos
# A condição é se a cidade mudou para marcar o registro antigo como inativo
condicao_mudanca = (clientes_atualizados_df.cliente_id == clientes_delta_df.cliente_id) & \
                   (clientes_atualizados_df.cidade != clientes_delta_df.cidade) & \
                   (clientes_delta_df.ativo == True)

# Marcar os registros antigos como inativos
clientes_historico_atualizado_df = clientes_delta_df.join(clientes_atualizados_df, "cliente_id", "left") \
    .withColumn("ativo", when(condicao_mudanca, lit(False)).otherwise(clientes_delta_df.ativo)) \
    .withColumn("data_fim", when(condicao_mudanca, current_date()).otherwise(clientes_delta_df.data_fim))

# 5. Inserir os novos registros ou registros alterados
clientes_novos_atualizados_df = clientes_atualizados_df.join(clientes_delta_df, "cliente_id", "leftanti") \
    .withColumn("ativo", lit(True)) \
    .withColumn("data_inicio", current_date()) \
    .withColumn("data_fim", lit(None).cast("date"))

# 6. Combinar os registros antigos (com atualizações) e os novos
clientes_final_df = clientes_historico_atualizado_df.unionByName(clientes_novos_atualizados_df)

# 7. Escrever o resultado final de volta para a tabela Delta
clientes_final_df.write.format("delta").mode("overwrite").save(delta_dir)

# 8. Exibir o resultado final com o histórico de clientes
print("Histórico de clientes com SCD Tipo 2 aplicado:")
clientes_result_df = spark.read.format("delta").load(delta_dir)
clientes_result_df.show(truncate=False)

# Encerrar a Spark Session
spark.stop()
