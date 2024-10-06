### Exemplo 2: **Transformações e Agregações em JSON**

Nesse exemplo, vamos trabalhar com um **JSON contendo dados de transações** de uma empresa. Nosso objetivo será:

1. **Filtrar** transações específicas.
2. **Agrupar e sumarizar** dados (agregações).
3. Preparar esses dados para serem carregados em um Data Lake ou processados em batch.


```json
{
  "transacoes": [
    {
      "id": 1,
      "cliente": "Rich",
      "valor": 1000,
      "data": "2024-01-15",
      "categoria": "Eletrônicos"
    },
    {
      "id": 2,
      "cliente": "Ana",
      "valor": 500,
      "data": "2024-01-16",
      "categoria": "Roupas"
    },
    {
      "id": 3,
      "cliente": "Carlos",
      "valor": 2000,
      "data": "2024-02-01",
      "categoria": "Eletrônicos"
    },
    {
      "id": 4,
      "cliente": "Rich",
      "valor": 300,
      "data": "2024-02-10",
      "categoria": "Alimentos"
    },
    {
      "id": 5,
      "cliente": "Ana",
      "valor": 700,
      "data": "2024-03-01",
      "categoria": "Eletrônicos"
    }
  ]
}

```



### Objetivos do Exemplo:

1. **Filtrar** todas as transações acima de um determinado valor (ex: transações acima de 1000).
2. **Somar o valor das transações por categoria** (Eletrônicos, Alimentos, etc.).
3. **Agrupar as transações por cliente** e calcular o total gasto por cada um.


### Explicação:

1. **Filtragem**:
    
    - Usamos uma list comprehension para filtrar transações acima de um valor definido (`valor > 1000`).
    - Essa técnica é muito útil ao lidar com grandes volumes de dados, pois você pode rapidamente isolar informações relevantes.
2. **Agregação por Categoria**:
    
    - Usamos um `defaultdict` para armazenar o total por categoria.
    - Iteramos sobre as transações e somamos o valor de cada transação na categoria correspondente.
3. **Agregação por Cliente**:
    
    - Novamente, usamos um `defaultdict` para acumular o total gasto por cada cliente.
    - Isso é essencial para consolidar os dados, especialmente ao trabalhar com relatórios ou para carregar em sistemas de análise.

### Próximos Passos:

Neste exemplo, você aprendeu a filtrar e agregar dados em JSON. No **Exemplo 3**, podemos expandir esse conceito trabalhando com **operações mais avançadas de transformação** de dados ou começando a preparar esses dados para o Spark, onde a escalabilidade será essencial.
