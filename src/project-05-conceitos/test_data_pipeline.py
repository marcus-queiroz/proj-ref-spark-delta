import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Configuração do Spark para o pytest
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("DataPipelineTests") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
        .getOrCreate()

# Testa se os dados de pedidos estão sendo lidos corretamente
def test_orders_data_load(spark):
    data_dir = "14-data"
    orders_path = os.path.join(data_dir, 'orders.csv')

    # Leitura dos dados de pedidos
    orders_df = spark.read.csv(orders_path, header=True, inferSchema=True)

    # Verifica se a tabela de pedidos tem as colunas esperadas
    expected_columns = ['order_id', 'customer_id', 'product_id', 'total_amount', 'order_status', 'order_date']
    assert all(col in orders_df.columns for col in expected_columns), "Faltando colunas nos dados de pedidos"
    
    # Verifica se o número de registros é maior que 0
    assert orders_df.count() > 0, "Os dados de pedidos estão vazios"
    
    # Verifica se todas as linhas têm order_id não-nulo
    assert orders_df.filter(col("order_id").isNull()).count() == 0, "Existem registros sem order_id"

# Testa se os dados de clientes estão sendo lidos corretamente
def test_customers_data_load(spark):
    data_dir = "14-data"
    customers_path = os.path.join(data_dir, 'customers.json')

    # Leitura dos dados de clientes
    customers_df = spark.read.json(customers_path)

    # Verifica se a tabela de clientes tem as colunas esperadas
    expected_columns = ['customer_id', 'customer_name', 'customer_address', 'email', 'registration_date']
    assert all(col in customers_df.columns for col in expected_columns), "Faltando colunas nos dados de clientes"
    
    # Verifica se o número de registros é maior que 0
    assert customers_df.count() > 0, "Os dados de clientes estão vazios"
    
    # Verifica se todas as linhas têm customer_id não-nulo
    assert customers_df.filter(col("customer_id").isNull()).count() == 0, "Existem registros sem customer_id"

# Testa se os dados de movimentação de estoque estão corretos
def test_inventory_movements_data_load(spark):
    data_dir = "14-data"
    inventory_movements_path = os.path.join(data_dir, 'inventory_movements.csv')

    # Leitura dos dados de movimentação de estoque
    inventory_df = spark.read.csv(inventory_movements_path, header=True, inferSchema=True)

    # Verifica se a tabela de movimentação tem as colunas esperadas
    expected_columns = ['movement_id', 'product_id', 'movement_type', 'quantity', 'movement_date']
    assert all(col in inventory_df.columns for col in expected_columns), "Faltando colunas nos dados de movimentação de estoque"
    
    # Verifica se o número de registros é maior que 0
    assert inventory_df.count() > 0, "Os dados de movimentação de estoque estão vazios"
    
    # Verifica se o tipo de movimentação é 'IN' ou 'OUT'
    assert inventory_df.filter(~col("movement_type").isin("IN", "OUT")).count() == 0, "Existem registros com movimento inválido"

# Testa se os dados de produtos estão sendo lidos corretamente
def test_products_data_load(spark):
    data_dir = "14-data"
    products_path = os.path.join(data_dir, 'products.csv')

    # Leitura dos dados de produtos
    products_df = spark.read.csv(products_path, header=True, inferSchema=True)

    # Verifica se a tabela de produtos tem as colunas esperadas
    expected_columns = ['product_id', 'product_name', 'category', 'price', 'storage_cost_per_unit']
    assert all(col in products_df.columns for col in expected_columns), "Faltando colunas nos dados de produtos"
    
    # Verifica se o número de registros é maior que 0
    assert products_df.count() > 0, "Os dados de produtos estão vazios"
    
    # Verifica se todas as linhas têm product_id não-nulo
    assert products_df.filter(col("product_id").isNull()).count() == 0, "Existem registros sem product_id"

# Testa se os dados de previsão de vendas estão corretos
def test_forecasted_sales_data_load(spark):
    data_dir = "14-data"
    forecasted_sales_path = os.path.join(data_dir, 'forecasted_sales.csv')

    # Leitura dos dados de previsão de vendas
    forecasted_sales_df = spark.read.csv(forecasted_sales_path, header=True, inferSchema=True)

    # Verifica se a tabela de previsão tem as colunas esperadas
    expected_columns = ['product_id', 'forecasted_quantity', 'forecast_date']
    assert all(col in forecasted_sales_df.columns for col in expected_columns), "Faltando colunas nos dados de previsão de vendas"
    
    # Verifica se o número de registros é maior que 0
    assert forecasted_sales_df.count() > 0, "Os dados de previsão de vendas estão vazios"
    
    # Verifica se as previsões são para datas futuras
    assert forecasted_sales_df.filter(col("forecast_date") < "2024-01-01").count() == 0, "Existem previsões com data no passado"

