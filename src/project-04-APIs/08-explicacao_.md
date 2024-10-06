Aqui está o código completo do **Exemplo 8 - Implementação de Carga Incremental com SCD Tipo 2**, que foca em trabalhar com o Slowly Changing Dimensions (SCD) Tipo 2 usando Delta Lake e PySpark.

### **Cenário:**

Vamos simular um cenário em que recebemos dados de transações e clientes que podem sofrer alterações com o tempo, e queremos armazenar o histórico dessas alterações, utilizando a abordagem SCD Tipo 2.

### **Passos do Exemplo 8:**

1. **Criação das tabelas iniciais**: Leitura dos dados de transações e clientes.
2. **Identificação de registros novos e alterados**: Compara os dados atuais com os dados previamente salvos para identificar atualizações.
3. **Implementação de SCD Tipo 2**: Para registros alterados, o registro antigo será marcado como inativo, e o novo registro será inserido como ativo.



### **Explicação do Exemplo 8:**

1. **Leitura e escrita inicial**: Simulamos um conjunto de dados de clientes e escrevemos na tabela Delta.
2. **Mudanças**: Simulamos um novo conjunto de dados que contém alterações, como mudanças de cidade ou novos clientes.
3. **Comparação**: Usamos uma condição para verificar se a cidade de um cliente mudou e, se mudou, marcamos o registro antigo como inativo e inserimos o novo registro com a cidade atualizada.
4. **Aplicação do SCD Tipo 2**: O histórico é mantido marcando os registros antigos como inativos, e os novos como ativos, com as colunas `data_inicio` e `data_fim` para rastrear as mudanças.
5. **Persistência no Delta Lake**: A tabela Delta armazena as mudanças e permite consultar o histórico completo.

### **Resultado Final**:

O Delta Lake armazenará o histórico de clientes, e você poderá ver as versões anteriores dos registros alterados. Esse padrão de SCD Tipo 2 é útil para manter o histórico de dados de clientes ou outros objetos de negócios que mudam com o tempo.

Se precisar de ajustes ou mais exemplos avançados, só avisar!

