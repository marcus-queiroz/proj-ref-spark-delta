from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_date, lit

# Criar a Spark session
spark = SparkSession.builder \
    .appName("CDC and SCD Simulation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Diretórios da camada Silver
silver_customers_path = "/workspace/14-data/silver/customers"
silver_orders_path = "/workspace/14-data/silver/orders"
silver_inventory_path = "/workspace/14-data/silver/inventory_movements"

# 1. Carga Incremental de Pedidos e Movimentações de Estoque
orders_incremental_df = spark.read.csv("/workspace/14-data/bronze/orders_incremental.csv", header=True, inferSchema=True)
inventory_incremental_df = spark.read.csv("/workspace/14-data/bronze/inventory_movements_incremental.csv", header=True, inferSchema=True)

# Função para realizar o merge incremental
def merge_incremental_data(delta_table_path, new_data_df, key_column):
    if DeltaTable.isDeltaTable(spark, delta_table_path):
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        delta_table.alias("target").merge(
            new_data_df.alias("source"),
            f"target.{key_column} = source.{key_column}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        new_data_df.write.format("delta").mode("overwrite").save(delta_table_path)

# Merge incremental para pedidos e movimentações
merge_incremental_data(silver_orders_path, orders_incremental_df, "order_id")
merge_incremental_data(silver_inventory_path, inventory_incremental_df, "movement_id")

# 2. Simulação de SCD Tipo 2 para Clientes
# Leitura da tabela de clientes atual
customers_silver_df = spark.read.format("delta").load(silver_customers_path)

# Dados incrementais de clientes (simulando mudanças nos dados dos clientes)
customers_incremental_df = spark.read.json("/workspace/14-data/bronze/customers_incremental.json")

# Criar a Delta Table para clientes
if DeltaTable.isDeltaTable(spark, silver_customers_path):
    delta_customers = DeltaTable.forPath(spark, silver_customers_path)
else:
    customers_incremental_df.write.format("delta").mode("overwrite").save(silver_customers_path)
    delta_customers = DeltaTable.forPath(spark, silver_customers_path)

# Aplicar SCD Tipo 2
# A estratégia será marcar o fim da validade do registro antigo quando houver uma atualização para o cliente
# e inserir um novo registro com a nova versão do cliente.
for row in customers_incremental_df.collect():
    customer_id = row["customer_id"]
    new_data = customers_incremental_df.filter(col("customer_id") == customer_id)

    # Fechar o registro anterior (marcar como expirado)
    delta_customers.update(
        condition=f"customer_id = {customer_id} AND current_flag = 'Y'",
        set={"current_flag": "'N'", "end_date": f"'{current_date()}'"}
    )

    # Inserir o novo registro com a atualização (novo endereço, email, etc.)
    new_data = new_data.withColumn("start_date", current_date()) \
                       .withColumn("end_date", lit(None)) \
                       .withColumn("current_flag", lit("Y"))
    new_data.write.format("delta").mode("append").save(silver_customers_path)

# Verificar os dados com SCD Tipo 2 aplicados
updated_customers_df = spark.read.format("delta").load(silver_customers_path)
updated_customers_df.show(truncate=False)

# Encerrar a Spark session
spark.stop()
