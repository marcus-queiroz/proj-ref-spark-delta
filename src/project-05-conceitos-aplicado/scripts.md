


## **Camada Bronze (Ingestão de Dados Brutos)**

### **Objetivo**:

A camada Bronze armazena dados brutos conforme são recebidos das diversas fontes de dados (vendas, clientes, movimentações de estoque). Aqui, os dados são capturados de forma incremental para garantir que novas inserções sejam persistidas, mas não há grandes transformações aplicadas.

### **Script: Camada Bronze (Ingestão de Dados)**

- **Fonte de dados**: Arquivos CSV e JSON.
    - Pedidos (`orders.csv`)
    - Movimentação de Estoque (`inventory_movements.csv`)
    - Informações de Clientes (`customers.json`)
- **Ingestão Incremental**:
    - A ingestão é feita incrementalmente, utilizando o timestamp (`data` ou `data_movimentacao`) para filtrar os dados que foram modificados ou adicionados desde a última execução.
- **Persistência**:
    - Os dados são armazenados em formato **Delta Lake**, o que permite operações futuras mais eficientes, como merge de dados ou consultas otimizadas.
```python
from pyspark.sql.functions import col

# Filtragem incremental com base no timestamp
novos_pedidos_df = pedidos_df.filter(col("data") > ultima_data_processada)
novas_movimentacoes_df = movimentacao_df.filter(col("data_movimentacao") > ultima_data_processada)

# Persistir dados brutos na camada Bronze (Delta Lake)
novos_pedidos_df.write.format("delta").mode("append").save("/path/to/delta/bronze/pedidos")
novas_movimentacoes_df.write.format("delta").mode("append").save("/path/to/delta/bronze/movimentacao")

```



### **Principais Responsabilidades**:

- **Captura incremental** dos dados brutos das fontes.
- **Persistência no formato Delta** para garantir maior eficiência nas operações futuras.
- Nenhuma transformação significativa é aplicada aos dados neste estágio, preservando sua integridade.

---

## **Camada Silver (Transformação e Integração de Dados)**

### **Objetivo**:

A camada Silver é responsável pela limpeza e transformação dos dados. Neste nível, removemos dados desnecessários ou inválidos (como pedidos cancelados) e integramos as diferentes fontes de dados (vendas, clientes, movimentação de estoques) para gerar tabelas mais consistentes e adequadas às análises de negócio.

### **Script: Camada Silver (Transformação e Integração)**

- **Limpeza de Dados**:
    - Remove dados inválidos, como pedidos cancelados ou incompletos, e ajusta campos que precisam ser padronizados.
- **Junção de Dados**:
    - Integra os dados de pedidos com os dados de clientes e movimentações de estoque, criando uma visão mais unificada.
- **Transformações**:
    - Implementa algumas transformações, como formatação de colunas de data, cálculo de novos campos derivados (por exemplo, tempo médio entre pedidos).
- **Persistência**:
    - Os dados transformados são novamente armazenados no Delta Lake, mas com mais qualidade e estrutura.
```python
from delta.tables import DeltaTable

# Realiza a junção entre pedidos e clientes
pedidos_com_clientes_df = pedidos_df.join(clientes_df, "cliente_id", "inner")

# Remove pedidos cancelados ou incompletos
pedidos_filtrados_df = pedidos_com_clientes_df.filter(col("status") != "CANCELADO")

# Salvar dados transformados na camada Silver
pedidos_filtrados_df.write.format("delta").mode("overwrite").save("/path/to/delta/silver/pedidos")
```


### **Principais Responsabilidades**:

- **Limpeza e padronização** dos dados para garantir consistência e qualidade.
- **Transformações** necessárias para enriquecer as tabelas de pedidos, clientes e estoques.
- **Junção de dados** entre as fontes, criando visões mais ricas e informativas.
- **Implementação de SCD Tipo 2**, para manter o histórico de alterações de clientes.

---

## **Camada Gold (Relatórios e Otimização)**

### **Objetivo**:

A camada Gold armazena dados prontos para análise e relatórios de alta performance. Os dados nessa camada foram otimizados para consultas complexas, utilizando particionamento e **Z-Ordering** para garantir que as consultas sejam rápidas e escaláveis. Além disso, essa camada traz uma visão consolidada e agregada dos dados.

### **Script: Camada Gold (Otimização e Agregação)**

- **Particionamento e Z-Ordering**:
    - Para melhorar a performance das consultas, os dados são particionados com base em colunas de data, permitindo que as consultas acessem apenas os blocos de dados necessários.
    - O **Z-Ordering** é aplicado para garantir que as consultas façam leituras otimizadas de disco, com base nas colunas que mais sofrem filtros (por exemplo, data, categoria de produto).
- **Relatórios Agregados**:
    - Agrega as vendas por categoria de produto, data, cliente, e fornece uma análise mais profunda do comportamento de vendas e movimentações de estoque.
```python
# Leitura dos dados da camada Silver
silver_data = spark.read.format("delta").load("/path/to/delta/silver/pedidos")

# Aplicar partições e Z-Ordering
silver_data.write.format("delta").partitionBy("data").option("optimizeWrite", "true").save("/path/to/delta/gold/pedidos")

# Otimização com Z-Ordering
delta_table_gold = DeltaTable.forPath(spark, "/path/to/delta/gold/pedidos")
delta_table_gold.optimize().executeZOrderBy("data")
```


### **Principais Responsabilidades**:

- **Otimização das consultas** através de técnicas de particionamento e Z-Ordering.
- **Agregação e análise** dos dados, permitindo a criação de relatórios detalhados sobre vendas, movimentação de estoque e comportamento de clientes.
- **Persistência em Delta Lake** com otimizações que garantem consultas eficientes em grandes volumes de dados.

---

### **Indicadores e Métricas Geradas**

1. **Análise de Vendas por Cliente**:
    - Valor total das vendas por cliente.
    - Histórico de vendas utilizando SCD Tipo 2 para acompanhar mudanças no perfil dos clientes.
2. **Análise de Movimentação de Estoque**:
    - Monitoramento da movimentação de estoque (entradas e saídas) para evitar ruptura de estoque.
3. **Otimização de Estoque**:
    - Relatórios que analisam a demanda por produto e sugerem ajustes nos níveis de estoque, prevenindo faltas ou excessos.





### Pytest

1. **Configuração do ambiente Spark**: Utiliza `pytest` com um `fixture` que inicializa a sessão Spark para ser usada nos testes.
2. **Testes por dataset**:
    - Para cada fonte de dados (pedidos, clientes, movimentação de estoque, produtos e previsões de vendas), o script:
        - Valida se os dados foram carregados corretamente.
        - Verifica se as colunas obrigatórias estão presentes.
        - Garante que o número de registros seja maior que zero.
        - Faz verificações adicionais para garantir a consistência, como se os IDs não estão nulos e se o tipo de movimentação está correto.

### Para rodar os testes:

1. Instale o `pytest` e as dependências do Spark:
```bash
pip install pytest
```
    
2. Execute os testes:
```bash
pytest test_data_pipeline.py
```
    
