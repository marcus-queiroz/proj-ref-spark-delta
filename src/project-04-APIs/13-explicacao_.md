### **Exemplo 13 - Aplicando Slowly Changing Dimension (SCD) Tipo 2 com Delta Lake**

No **Exemplo 13**, vamos abordar um conceito fundamental para ambientes de **Data Warehousing**: o tratamento de **Slowly Changing Dimensions (SCD)**, especificamente o **SCD Tipo 2**. Esse tipo de dimensão é utilizado quando precisamos manter o histórico das alterações de dados de uma dimensão ao longo do tempo.

### **Objetivos do Exemplo 13:**

1. **Implementar o SCD Tipo 2**: Criar um processo que permita rastrear alterações em uma dimensão e manter o histórico completo dessas mudanças.
2. **Utilizar Delta Lake**: Aproveitar os recursos de versionamento e controle de histórico do Delta Lake para gerenciar as mudanças incrementais.
3. **Eficiência e Controle de Histórico**: Garantir que o histórico das dimensões seja preservado e que o processo de ETL seja eficiente.

### **Cenário:**

Neste exemplo, vamos trabalhar com uma tabela de **clientes**. Suponha que as informações dos clientes mudem ao longo do tempo (ex.: cidade, telefone, etc.). O objetivo é registrar cada mudança no perfil dos clientes, mantendo o histórico de todas as versões de cada cliente.


### **Explicação do Exemplo 13:**

1. **Leitura de Dados Atualizados**: O script começa lendo um arquivo CSV contendo as informações mais recentes dos clientes. Esses dados podem incluir mudanças nas informações pessoais, como o nome ou a cidade.
    
2. **Leitura de Dados Existentes**: A tabela de clientes existente é lida do **Delta Lake**, que contém todas as versões anteriores dos clientes.
    
3. **Marcação de Registros Expirados**: Clientes que sofreram mudanças são marcados como **expirados**. Isso é feito utilizando a coluna `is_current`, que é ajustada para `False` para indicar que o registro não está mais ativo.
    
4. **Criação de Novas Versões**: Para os clientes que sofreram alterações, novas versões dos registros são criadas, com as informações atualizadas e as colunas de data `valid_from` e `valid_until` sendo ajustadas para manter o histórico.
    
5. **Persistência no Delta Lake**: O novo conjunto de registros é escrito no **Delta Lake**, preservando o histórico completo dos clientes.
    

### **SCD Tipo 2:**

O **SCD Tipo 2** permite preservar todas as versões anteriores de um registro, adicionando novas linhas quando há mudanças. No caso deste exemplo:

- **is_current**: Indica se o registro está ativo (`True`) ou se já foi substituído por uma nova versão (`False`).
- **valid_from** e **valid_until**: Essas colunas permitem saber exatamente quando o registro começou e terminou a ser válido.

### **Conclusão:**

O **Exemplo 13** demonstra como aplicar o conceito de **Slowly Changing Dimensions (SCD) Tipo 2** com o **Delta Lake** para gerenciar mudanças em dimensões de forma eficiente e mantendo o histórico. Este padrão é amplamente utilizado em **Data Warehousing** para garantir que todo o histórico de alterações seja preservado e facilmente consultável.