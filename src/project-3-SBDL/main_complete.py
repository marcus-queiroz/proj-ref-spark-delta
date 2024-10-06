import sys
import uuid
from lib import ConfigLoader, Utils, DataLoader #,Transformations
from lib.logger import Log4j
from pyspark.sql.functions import struct, lit, col, array, when, isnull, filter, current_timestamp, date_format, expr, collect_list

# Definindo o ambiente de execução e a data de carregamento
# Aqui, o ambiente é configurado como "LOCAL", e a data é fixada
job_run_env = "LOCAL"  # Definindo o ambiente como 'local'
load_date = "2024-10-05"    # Data fixa para carregamento

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
accounts_df.show(5)  
accounts_df.printSchema()  
def get_contract(df):
    contract_title = array(when(~isnull("legal_title_1"),
                                struct(lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                                       col("legal_title_1").alias("contractTitleLine")).alias("contractTitle")),
                           when(~isnull("legal_title_2"),
                                struct(lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                                       col("legal_title_2").alias("contractTitleLine")).alias("contractTitle"))
                           )

    contract_title_nl = filter(contract_title, lambda x: ~isnull(x))

    tax_identifier = struct(col("tax_id_type").alias("taxIdType"),
                            col("tax_id").alias("taxId")).alias("taxIdentifier")

    return df.select("account_id", get_insert_operation(col("account_id"), "contractIdentifier"),
                     get_insert_operation(col("source_sys"), "sourceSystemIdentifier"),
                     get_insert_operation(col("account_start_date"), "contactStartDateTime"),
                     get_insert_operation(contract_title_nl, "contractTitle"),
                     get_insert_operation(tax_identifier, "taxIdentifier"),
                     get_insert_operation(col("branch_code"), "contractBranchCode"),
                     get_insert_operation(col("country"), "contractCountry"),
                     )

def get_insert_operation(column, alias):
    return struct(lit("INSERT").alias("operation"),
                  column.alias("newValue"),
                  lit(None).alias("oldValue")).alias(alias)

# Aplica transformações para criar um DataFrame de contratos a partir das contas
contract_df = get_contract(accounts_df)
contract_df.show(5)
contract_df.printSchema()

# Carrega os dados das partes envolvidas (DataFrame de clientes ou entidades relacionadas)
logger.info("Reading SBDL Party DF")
parties_df = DataLoader.read_parties(spark, job_run_env, enable_hive, hive_db)
parties_df.show(5)
parties_df.printSchema()

def get_relations(df):
    return df.select("account_id", "party_id",
                     get_insert_operation(col("party_id"), "partyIdentifier"),
                     get_insert_operation(col("relation_type"), "partyRelationshipType"),
                     get_insert_operation(col("relation_start_date"), "partyRelationStartDateTime")
                     )
# Aplica transformações para obter as relações entre essas partes
relations_df = get_relations(parties_df)
relations_df.show(5)
relations_df.printSchema()


# Carrega os dados de endereços (DataFrame de endereços)
logger.info("Reading SBDL Address DF")
address_df = DataLoader.read_address(spark, job_run_env, enable_hive, hive_db)
address_df.show(5)
address_df.printSchema()
def get_address(df):
    address = struct(col("address_line_1").alias("addressLine1"),
                     col("address_line_2").alias("addressLine2"),
                     col("city").alias("addressCity"),
                     col("postal_code").alias("addressPostalCode"),
                     col("country_of_address").alias("addressCountry"),
                     col("address_start_date").alias("addressStartDate")
                     )

    return df.select("party_id", get_insert_operation(address, "partyAddress"))
# Aplica transformações para obter o DataFrame de endereços relacionados
relation_address_df = get_address(address_df)
relation_address_df.show(5)
relation_address_df.printSchema()


def join_party_address(p_df, a_df):
    return p_df.join(a_df, "party_id", "left_outer") \
        .groupBy("account_id") \
        .agg(collect_list(struct("partyIdentifier",
                                 "partyRelationshipType",
                                 "partyRelationStartDateTime",
                                 "partyAddress"
                                 ).alias("partyDetails")
                          ).alias("partyRelations"))
# Junta os dados de relações de partes com os endereços para criar um DataFrame único
logger.info("Join Party Relations and Address")
party_address_df = join_party_address(relations_df, relation_address_df)
party_address_df.show(5)
party_address_df.printSchema()


def join_contract_party(c_df, p_df):
    return c_df.join(p_df, "account_id", "left_outer")
# Junta os dados de contratos com os dados de partes e endereços
logger.info("Join Account and Parties")
data_df = join_contract_party(contract_df, party_address_df)
data_df.show(5)
data_df.printSchema()

def apply_header(spark, df):
    header_info = [("SBDL-Contract", 1, 0), ]
    header_df = spark.createDataFrame(header_info) \
        .toDF("eventType", "majorSchemaVersion", "minorSchemaVersion")

    event_df = header_df.hint("broadcast").crossJoin(df) \
        .select(struct(expr("uuid()").alias("eventIdentifier"),
                       col("eventType"), col("majorSchemaVersion"), col("minorSchemaVersion"),
                       lit(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")).alias("eventDateTime")
                       ).alias("eventHeader"),
                array(struct(lit("contractIdentifier").alias("keyField"),
                             col("account_id").alias("keyValue")
                             )).alias("keys"),
                struct(col("contractIdentifier"),
                       col("sourceSystemIdentifier"),
                       col("contactStartDateTime"),
                       col("contractTitle"),
                       col("taxIdentifier"),
                       col("contractBranchCode"),
                       col("contractCountry"),
                       col("partyRelations")
                       ).alias("payload")
                )
    return event_df
    
# Aplica um cabeçalho e cria um evento (provavelmente adiciona metadados ou estrutura padrão)
logger.info("Apply Header and create Event")
final_df = apply_header(spark, data_df)
final_df.show(5)
final_df.printSchema()

