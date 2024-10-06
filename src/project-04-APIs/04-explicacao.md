### Exemplo 4: **Integração de Múltiplos Arquivos JSON**

Nesse exemplo, vamos trabalhar com múltiplos arquivos JSON contendo dados de **transações e clientes**. Nosso objetivo será:

1. **Carregar múltiplos arquivos JSON**.
2. **Integrar (juntar) dados** de diferentes fontes.
3. **Limpar dados duplicados** ou desnecessários antes de carregá-los.

### JSONs de Exemplo:

#### Arquivo 1: `transacoes.json`

```json
{
  "transacoes": [
    {
      "id": 1,
      "cliente_id": 101,
      "valor": 1000,
      "data": "2024-01-15",
      "categoria": "Eletrônicos"
    },
    {
      "id": 2,
      "cliente_id": 102,
      "valor": 500,
      "data": "2024-01-16",
      "categoria": "Roupas"
    }
  ]
}

```

#### Arquivo 2: `clientes.json`
```json
{
  "clientes": [
    {
      "cliente_id": 101,
      "nome": "Rich",
      "idade": 30,
      "cidade": "São Paulo"
    },
    {
      "cliente_id": 102,
      "nome": "Ana",
      "idade": 28,
      "cidade": "Rio de Janeiro"
    },
    {
      "cliente_id": 103,
      "nome": "Carlos",
      "idade": 35,
      "cidade": "Belo Horizonte"
    }
  ]
}

```


### Objetivos do Exemplo:

1. **Carregar múltiplos arquivos JSON** contendo informações de transações e clientes.
2. **Combinar os dados** usando uma chave de relacionamento (`cliente_id`).
3. **Eliminar duplicatas** e manter dados limpos para análise ou carregamento no pipeline.



### Explicação:

1. **Carregar múltiplos arquivos JSON**:
    
    - Simulamos a leitura de dois arquivos JSON: um contendo as transações e o outro contendo informações dos clientes.
    - Estes dois arquivos têm uma chave comum chamada `cliente_id`, que usaremos para integrar os dados.
2. **Integração (juntar os dados)**:
    
    - Utilizamos um laço `for` nas transações e, para cada transação, buscamos as informações correspondentes do cliente utilizando `cliente_id`.
    - A integração é feita combinando os dicionários de transações e clientes usando `{**dicionário1, **dicionário2}` para mesclar os dados.
3. **Limpeza de duplicatas**:
    
    - Para garantir que não tenhamos clientes duplicados, criamos um `set` para registrar os IDs de clientes já vistos. Se encontrarmos um cliente repetido, ele é ignorado.

### Próximos Passos:

Neste exemplo, você aprendeu a integrar múltiplos arquivos JSON com base em uma chave de relacionamento, além de realizar uma limpeza básica de duplicatas. Esse conceito é extremamente importante quando lidamos com dados distribuídos em vários arquivos ou fontes, uma situação comum em pipelines de dados.

No **Exemplo 5**, vamos começar a preparar esses dados para o Spark, explorando o carregamento e manipulação de grandes volumes de dados em um ambiente distribuído. Isso será uma introdução ao **processamento em lote** e às capacidades escaláveis do Spark.