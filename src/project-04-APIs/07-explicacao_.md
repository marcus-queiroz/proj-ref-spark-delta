### Exemplo 6: **Construção de um Pipeline ETL Completo com Spark e Databricks (Usando Spark Localmente)**

### Objetivo:

- Criar um **pipeline ETL completo** usando o Spark, simulando o fluxo de extração de dados de múltiplas fontes, transformações complexas e carregamento final.
- Aplicar operações como **joins**, **agregações** e **tratamento de dados faltantes** (missing data).
- Utilizar **caminhos relativos** para garantir a flexibilidade e reusabilidade do código.
- Preparar o pipeline para ser executado localmente e em ambientes como **Databricks**.

### Estrutura das Pastas:
```json
06-data/
    ├── 06-src-clientes.json
    ├── 06-src-transacoes.csv
    ├── 06-src-produtos.csv
    └── 06-dst/
```

### Exemplos de Arquivos de Entrada:

#### Arquivo JSON: `06-src-clientes.json`
```json
[
  {"id": 1, "nome": "Rich", "idade": 30, "cidade": "São Paulo"},
  {"id": 2, "nome": "Ana", "idade": 28, "cidade": "Rio de Janeiro"},
  {"id": 3, "nome": "Carlos", "idade": 35, "cidade": "Belo Horizonte"},
  {"id": 4, "nome": "Julia", "idade": null, "cidade": "Brasília"}
]
```


#### Arquivo CSV: `06-src-transacoes.csv`
```json
id,cliente_id,produto_id,valor,data,categoria
1,1,101,1000.50,2024-01-15,Eletrônicos
2,2,102,250.75,2024-02-10,Roupas
3,3,103,500.00,2024-03-05,Alimentos
4,1,101,300.00,2024-04-01,Eletrônicos
5,4,104,1200.00,2024-04-10,Viagem
```


#### Arquivo CSV: `06-src-produtos.csv`
```json
produto_id,produto_nome,preco
101,Notebook,2000.00
102,Camisa,50.00
103,Arroz,20.00
104,Pacote de Viagem,1500.00
```


### Passos do Pipeline ETL:

1. **Extração**:
    
    - Ler os dados de clientes (JSON), transações (CSV) e produtos (CSV).
2. **Transformação**:
    
    - **Filtrar** e corrigir valores faltantes (idade de clientes e outras possíveis inconsistências).
    - **Juntar** as transações com informações de clientes e produtos.
    - **Calcular novos valores**: Exemplo, a diferença entre o valor da transação e o preço original do produto.
3. **Carregamento**:
    
    - Salvar o resultado das transformações em arquivos processados na pasta `06-dst/`.


---
### Explicação dos Passos:

1. **Construção de Caminhos Relativos**:
    
    - Usamos o módulo **`os`** para definir os caminhos relativos ao diretório onde o script está sendo executado, garantindo flexibilidade em diferentes ambientes.
2. **Criação da Spark Session**:
    
    - Inicializamos uma Spark Session com configuração para otimizar o número de partições.
3. **Extração (Leitura dos Arquivos)**:
    
    - **`clientes.json`**: Carrega informações de clientes, incluindo nome, idade e cidade.
    - **`transacoes.csv`**: Carrega dados de transações, incluindo valores, datas e categorias.
    - **`produtos.csv`**: Carrega informações dos produtos relacionados às transações.
4. **Transformação (Processamento dos Dados)**:
    
    - **Tratamento de valores faltantes**: Corrigimos a coluna de idade no DataFrame de clientes, substituindo valores `null` por um valor padrão de 25 anos.
    - **Join (Junção) entre DataFrames**:
        - Primeiro, unimos as transações com os clientes usando `cliente_id`.
        - Em seguida, unimos o resultado com o DataFrame de produtos usando `produto_id`.
    - **Criação de Coluna Calculada**: Adicionamos uma nova coluna `diferenca_valor_preco`, calculando a diferença entre o valor da transação e o preço original do produto.
5. **Carregamento (Salvar os Resultados)**:
    
    - Os resultados processados são salvos no diretório de destino `06-dst/` usando o caminho relativo.
6. **Encerramento da Spark Session**:
    
    - Finalizamos a Spark Session com `spark.stop()`.

---

### O que Você Aprendeu:

- Construção de um pipeline ETL completo, incluindo **extração**, **transformação** e **carregamento** de dados.
- Uso de **joins** entre múltiplas fontes de dados.
- Como tratar dados faltantes no Spark.
- Criação de colunas calculadas e operações aritméticas em DataFrames.
- Salvar os dados processados de forma eficiente usando **caminhos relativos**.