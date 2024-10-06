
**Exemplo 9 - Carga Incremental com SCD Tipo 2 e Particionamento usando Delta Lake**

O foco do **Exemplo 9** é aumentar a eficiência das consultas e atualizações ao introduzir o **particionamento** na tabela Delta. Particionar os dados melhora o desempenho de leitura e escrita, especialmente em grandes volumes de dados. Neste exemplo, usaremos a abordagem SCD Tipo 2, combinada com o particionamento por colunas relevantes.

### **Cenário:**

Neste exemplo, simulamos transações de clientes e usamos a abordagem SCD Tipo 2 com particionamento para armazenar o histórico de alterações em Delta Lake. Vamos particionar os dados pela coluna `cidade` para otimizar as consultas e operações de carga.

### **Objetivos do Exemplo 9:**

1. **Aplicar SCD Tipo 2**: Continuar com a abordagem de Slowly Changing Dimensions.
2. **Usar Particionamento**: Aplicar particionamento para melhorar a performance das operações no Delta Lake.
3. **Carga Incremental**: Realizar carga incremental para simular a entrada contínua de dados.




### **Explicação do Exemplo 9:**

1. **Criação da tabela inicial**: Inserimos os dados de clientes e aplicamos as colunas `ativo`, `data_inicio`, e `data_fim`, marcando todos como registros ativos.
2. **Simulação de alterações**: Introduzimos alterações nos clientes, como mudanças de cidade e novos clientes.
3. **SCD Tipo 2 com Particionamento**: Aplicamos a lógica de SCD Tipo 2, onde registros antigos são marcados como inativos, e novos registros são inseridos como ativos. O Delta Lake armazena essas mudanças.
4. **Particionamento**: Os dados são particionados pela coluna `cidade`, o que melhora o desempenho em operações de leitura e escrita, principalmente quando filtramos por cidade.
5. **Persistência e Leitura**: Escrevemos os dados finais de volta no Delta Lake com particionamento, e exibimos o histórico completo de alterações.

### **Vantagens do Particionamento:**

- **Desempenho**: Particionar dados em uma tabela Delta otimiza consultas ao reduzir a quantidade de dados processados em operações de leitura.
- **Carga Incremental**: Para cada novo conjunto de dados, o particionamento permite que apenas as partições afetadas sejam atualizadas, acelerando o processo.

### **Resultado Final**:

A tabela Delta armazenará os dados de clientes com histórico de alterações, mantendo o desempenho graças ao particionamento por `cidade`. Esse exemplo é ideal para cenários onde as alterações nos dados são frequentes e o volume de dados é significativo.

Se precisar de ajustes ou quiser mais detalhes, só me avisar!