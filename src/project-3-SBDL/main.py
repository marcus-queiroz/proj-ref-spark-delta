import sys
import uuid
from pyspark.sql.functions import struct, col, to_json
from lib import ConfigLoader, Utils, DataLoader, Transformations
from lib.logger import Log4j
import json
import os

if __name__ == '__main__':

    # Definindo o ambiente de execução e a data de carregamento
    # Se não for LOCAL, outras configurações são aplicadas ao cluster Spark
    job_run_env = "LOCAL"  
    load_date = "2024-10-05"  

    # Gerando um identificador único para esta execução do job
    job_run_id = "SBDL-" + str(uuid.uuid4())  

    # Imprime no console o início da execução do job
    print(f"Initializing SBDL Job in {job_run_env} Job ID: {job_run_id}")

    # Carrega as configurações do arquivo correspondente ao ambiente (ex.: conf/local.conf)
    conf = ConfigLoader.get_config(job_run_env)

    # Verifica se o Hive está habilitado nas configurações, para decidir se será utilizado
    enable_hive = True if conf["enable.hive"] == "true" else False
    hive_db = conf["hive.database"]  # Obtém o nome do banco de dados Hive das configurações

    # Inicializando a sessão Spark, essencial para processamento distribuído de dados
    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)  # Cria a sessão Spark apropriada ao ambiente

    # Inicializa o logger para registrar informações no log
    logger = Log4j(spark)



    # Carrega os dados das contas do sistema (DataFrame de contas)
    logger.info("Reading SBDL Account DF")
    accounts_df = DataLoader.read_accounts(spark, job_run_env, enable_hive, hive_db)

    # Aplica transformações para criar um DataFrame de contratos a partir das contas
    contract_df = Transformations.get_contract(accounts_df)

    # Carrega os dados das partes envolvidas (DataFrame de clientes ou entidades relacionadas)
    logger.info("Reading SBDL Party DF")
    parties_df = DataLoader.read_parties(spark, job_run_env, enable_hive, hive_db)

    # Aplica transformações para obter as relações entre essas partes
    relations_df = Transformations.get_relations(parties_df)

    # Carrega os dados de endereços (DataFrame de endereços)
    logger.info("Reading SBDL Address DF")
    address_df = DataLoader.read_address(spark, job_run_env, enable_hive, hive_db)

    # Aplica transformações para obter o DataFrame de endereços relacionados
    relation_address_df = Transformations.get_address(address_df)

    # Junta os dados de relações de partes com os endereços para criar um DataFrame único
    logger.info("Join Party Relations and Address")
    party_address_df = Transformations.join_party_address(relations_df, relation_address_df)

    # Junta os dados de contratos com os dados de partes e endereços
    logger.info("Join Account and Parties")
    data_df = Transformations.join_contract_party(contract_df, party_address_df)

    # Aplica um cabeçalho e cria um evento (provavelmente adiciona metadados ou estrutura padrão)
    logger.info("Apply Header and create Event")
    final_df = Transformations.apply_header(spark, data_df)

    # Verifica se a pasta 'output' existe, e cria se necessário
    output_dir = 'test_data/output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Converte o DataFrame para uma lista de strings JSON
    data_as_json = final_df.toJSON().collect()

    # Adicione um log para ver se os dados foram coletados
    print(f"Total de registros coletados: {len(data_as_json)}")

    # Salva o resultado como um arquivo JSON local
    output_file_path = os.path.join(output_dir, 'final_df_unified.json')
    with open(output_file_path, 'w') as f:
        json.dump(data_as_json, f, indent=4)

    # Adicione um log para confirmar o salvamento
    print(f"Arquivo salvo em: {output_file_path}")






    # Prepara os dados para serem enviados ao Kafka
    logger.info("Preparing to send data to Kafka")
    kafka_kv_df = final_df.select(
        col("payload.contractIdentifier.newValue").alias("key"),  # Define a chave como o identificador de contrato
        to_json(struct("*")).alias("value")  # Converte todo o DataFrame para JSON
    )

    # Aguarda a entrada do usuário antes de continuar (provavelmente para verificação)
    #input("Press Any Key")

    # Configurações de segurança e autenticação para o Kafka
    # As chaves são armazenadas de forma segura nas configurações (não hardcoded no código)
    api_key = conf["kafka.api_key"]
    api_secret = conf["kafka.api_secret"]

    # Envia os dados para o Kafka usando as configurações recuperadas
    """kafka_kv_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
        .option("topic", conf["kafka.topic"]) \
        .option("kafka.security.protocol", conf["kafka.security.protocol"]) \
        .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(api_key, api_secret)) \
        .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"]) \
        .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"]) \
        .save()"""

    # Confirma que os dados foram enviados para o Kafka
    logger.info("Finished sending data to Kafka")
