### Explicação dos Passos:

1. **Construir Caminhos Relativos**:
    
    - Usamos o módulo **`os`** para obter o diretório de execução do script com `os.path.dirname(os.path.realpath(__file__))`.
    - A partir do diretório atual, construímos o caminho relativo para a pasta de dados (`05-data`) e os arquivos de origem (`05-src-clientes.json` e `05-src-transacoes.csv`).
2. **Criar uma Spark Session**:
    
    - A **Spark Session** é o ponto de entrada para o Spark e pode ser configurada para otimizar o número de partições, reduzindo a quantidade de partições em operações de agregação, otimizando o processamento em um ambiente local.
3. **Leitura dos Arquivos JSON e CSV**:
    
    - Usamos `spark.read.json()` e `spark.read.csv()` para ler os arquivos localizados na pasta `05-data/` com os caminhos relativos construídos dinamicamente.
4. **Transformações Intermediárias e Avançadas**:
    
    - **Filtragem e Ordenação**: Filtramos as transações com valor acima de 500 e ordenamos em ordem decrescente pelo valor.
    - **Renomeação de Colunas**: Para facilitar a leitura e manipulação dos dados, renomeamos as colunas usando `alias()`.
    - **Junção (Join)**: Realizamos um **inner join** entre as transações e os clientes, usando a coluna `cliente_id` como chave.
    - **Agregação**: Calculamos o valor total de transações por categoria com `groupBy` e `sum()`.
5. **Otimização de Salvamento**:
    
    - **Particionamento**: Durante a gravação, usamos `partitionBy()` para salvar os arquivos de saída particionados por categoria, melhorando a performance de futuras consultas.
    - Os arquivos processados são salvos em `05-dst-transacoes_filtradas.csv` e `05-dst-total_por_categoria.json` usando caminhos relativos.
6. **Encerrar a Spark Session**:
    
    - A Spark Session é finalizada com `spark.stop()` para liberar os recursos.

### O que Você Aprendeu:

- Como construir **caminhos relativos** usando a biblioteca `os` para garantir que os arquivos sejam acessados de forma correta, independentemente do ambiente de execução.
- Leitura e processamento de arquivos **JSON** e **CSV** em Spark usando caminhos relativos.
- Operações intermediárias e avançadas em **DataFrames**, incluindo **filtros**, **joins** e **agregações**.
- Melhores práticas para salvar resultados processados, incluindo **particionamento**.

### Próximos Passos:

No **Exemplo 6**, exploraremos a construção de um **pipeline ETL completo**, integrando múltiplas fontes de dados e transformações mais complexas, preparando o caminho para o uso do Spark no **Databricks**.
