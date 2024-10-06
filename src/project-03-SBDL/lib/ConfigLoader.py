import configparser
import os
from pyspark import SparkConf


def get_config(env):
    config = configparser.ConfigParser()  # Inicializa um parser de configuração
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'conf', 'sbdl.conf'))
    config.read(config_path)  # Lê o arquivo 'sbdl.conf' na pasta 'conf'

    conf = {}  
    for (key, val) in config.items(env):  # Itera sobre os pares de chave-valor do ambiente especificado
        conf[key] = val  # Adiciona cada configuração no dicionário 'conf'

    return conf  


def get_spark_conf(env):
    spark_conf = SparkConf()  # Inicializa um objeto SparkConf, que guarda as configurações do Spark
    config = configparser.ConfigParser()  # Inicializa o parser de configuração
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'conf', 'spark.conf'))
    config.read(config_path)  # Lê o arquivo 'spark.conf' para configurar o Spark

    for (key, val) in config.items(env):  # Itera sobre os pares chave-valor do ambiente específico
        spark_conf.set(key, val)  # Define cada configuração no SparkConf

    return spark_conf  


def get_data_filter(env, data_filter):
    conf = get_config(env)  # Obtém as configurações do ambiente usando a função get_config()
    return "true" if conf[data_filter] == "" else conf[data_filter]  # Retorna "true" se o filtro estiver vazio, caso contrário, retorna o valor do filtro
