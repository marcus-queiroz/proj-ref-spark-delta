from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_date, lit, expr

# Criar a Spark session
spark = SparkSession.builder \
    .appName("SCD Type 2 Simulation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Diretório dos dados
silver_customers_path = "/workspace/14-data/silver/customers"

# Função para implementar SCD Tipo 2
def apply_scd2(delta_table, new_data, key_column, updated_columns):
    # Buscar a tabela Delta existente
    existing_data = delta_table.toDF()

    # Atualizar registros anteriores como inativos
    updates = new_data.alias("source").join(existing_data.alias("target"),
                                            on=[f"source.{key_column} = target.{key_column}",
                                                *[
                                                    f"source.{col} <> target.{col}" for col in updated_columns
                                                ]])

    updates_to_expire = updates.selectExpr("target.*").withColumn("current", lit(False)).withColumn("end_date", current_date())

    # Inserir novos registros com status ativo
    new_active = updates.selectExpr("source.*").withColumn("current", lit(True)).withColumn("start_date", current_date()).withColumn("end_date", lit(None))

    # Combinar as mudanças (expirar antigos e adicionar novos)
    merged_data = existing_data.union(updates_to_expire).union(new_active)

    # Atualizar a tabela Delta com os dados mesclados
    delta_table.alias("target").merge(
        merged_data.alias("source"),
        f"target.{key_column} = source.{key_column}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Leitura dos clientes existentes da camada Silver
if DeltaTable.isDeltaTable(spark, silver_customers_path):
    customers_silver = DeltaTable.forPath(spark, silver_customers_path)
    customers_silver_df = customers_silver.toDF()
else:
    # Caso não exista, criar a primeira tabela Delta na Silver
    customers_silver_df = spark.read.json("/workspace/14-data/customers_initial.json")
    customers_silver_df = customers_silver_df.withColumn("current", lit(True)).withColumn("start_date", current_date()).withColumn("end_date", lit(None))
    customers_silver_df.write.format("delta").mode("overwrite").save(silver_customers_path)
    customers_silver = DeltaTable.forPath(spark, silver_customers_path)

# Simular mudanças nos clientes (como mudança de endereço)
updated_customers = [
    (1, "Alice", "alice@example.com", "New York", "2024-09-05"),  # Novo endereço
    (3, "Bob", "bob@example.com", "San Francisco", "2024-09-05"),  # Novo cliente
    (2, "Charlie", "charlie@example.com", "Los Angeles", "2024-09-05")  # Novo cliente
]

columns = customers_silver_df.columns[:-3]  # Excluindo current, start_date, end_date
updated_customers_df = spark.createDataFrame(updated_customers, schema=columns)

# Aplicar SCD Tipo 2 na tabela Delta
apply_scd2(customers_silver, updated_customers_df, "customer_id", ["name", "email", "address"])

# Verificar os dados na camada Silver após a aplicação do SCD Tipo 2
silver_scd_df = spark.read.format("delta").load(silver_customers_path)
silver_scd_df.show(truncate=False)

# Encerrar a Spark session
spark.stop()
