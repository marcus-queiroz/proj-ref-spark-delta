# Importar as bibliotecas necessárias
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc

# 1. Definir o caminho relativo baseado no diretório de execução
current_dir = os.path.dirname(os.path.realpath(__file__))  # Diretório onde o script está sendo executado
data_dir = os.path.join(current_dir, '05-data')  # Caminho relativo para a pasta de dados

clientes_json_path = os.path.join(data_dir, '05-src-clientes.json')
transacoes_csv_path = os.path.join(data_dir, '05-src-transacoes.csv')

# 2. Criar uma Spark Session
spark = SparkSession.builder \
    .appName("Exemplo5-SparkLocal") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# 3. Leitura dos arquivos JSON e CSV com caminho relativo
# Leitura do arquivo JSON (clientes.json)
clientes_df = spark.read.json(clientes_json_path)

# Leitura do arquivo CSV (transacoes.csv)
transacoes_df = spark.read.csv(transacoes_csv_path, header=True, inferSchema=True)

# 4. Visualizar os dados
print("Dados dos Clientes:")
clientes_df.show(truncate=False)

print("Dados das Transações:")
transacoes_df.show(truncate=False)

# 5. Realizar transformações intermediárias e avançadas

# Filtrar transações de valor acima de 500 e ordenar pelo valor em ordem decrescente
transacoes_filtradas = transacoes_df.filter(col("valor") > 500).orderBy(desc("valor"))
print("Transações com valor acima de 500 (ordenado por valor):")
transacoes_filtradas.show(truncate=False)

# Selecionar colunas específicas do DataFrame de transações e renomear para facilitar
transacoes_selecionadas = transacoes_df.select(
    col("id").alias("transacao_id"),
    col("cliente_id"),
    col("valor"),
    col("categoria")
)
print("Transações (colunas selecionadas e renomeadas):")
transacoes_selecionadas.show(truncate=False)

# Realizar um join (junção) entre as transações e os dados de clientes
transacoes_com_clientes = transacoes_selecionadas.join(clientes_df, transacoes_selecionadas.cliente_id == clientes_df.id, "inner")

print("Transações com informações de clientes:")
transacoes_com_clientes.select(
    "transacao_id", "nome", "valor", "categoria", "cidade"
).show(truncate=False)

# Agregar dados somando o valor total por categoria
total_por_categoria = transacoes_df.groupBy("categoria").agg(_sum("valor").alias("valor_total"))

print("Total de Transações por Categoria:")
total_por_categoria.show(truncate=False)

# 6. Salvar os resultados processados
# Caminhos de destino
dst_transacoes_filtradas = os.path.join(data_dir, '05-dst-transacoes_filtradas.csv')
dst_total_por_categoria = os.path.join(data_dir, '05-dst-total_por_categoria.json')

# Salvar transações filtradas em um novo arquivo CSV (particionando por categoria) no caminho relativo
transacoes_filtradas.write.mode("overwrite").partitionBy("categoria").csv(dst_transacoes_filtradas, header=True)

# Salvar o total por categoria em um arquivo JSON no caminho relativo
total_por_categoria.write.mode("overwrite").json(dst_total_por_categoria)

# Encerrar a Spark Session
spark.stop()
