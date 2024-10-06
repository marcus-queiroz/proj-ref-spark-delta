### **Exemplo 11 - Gerenciamento de Slowly Changing Dimensions (SCD) Tipo 2 com Delta Lake**

No **Exemplo 11**, vamos abordar um conceito essencial em **Data Warehousing**: o **gerenciamento de dimensões que mudam lentamente (SCD - Slowly Changing Dimensions)**. Esse é um padrão utilizado para registrar mudanças em dimensões ao longo do tempo, garantindo que tanto o histórico quanto o estado atual dos dados sejam preservados.

O **SCD Tipo 2** é uma técnica que cria novas versões dos registros de dimensão sempre que ocorre uma alteração. Cada registro novo mantém um histórico de suas modificações, com marcadores de tempo para indicar o período em que cada versão esteve ativa.

### **Objetivos do Exemplo 11:**

1. **Implementação do SCD Tipo 2**: Gerenciar alterações nas dimensões criando versões históricas de cada registro modificado.
2. **Uso do Delta Lake para Versionamento**: O Delta Lake facilita o controle de versões dos registros, além de oferecer funcionalidades de merge e controle temporal.
3. **Manutenção de Dados Históricos**: Implementar lógica para armazenar e manter registros históricos de clientes, assegurando que cada versão do cliente seja preservada ao longo do tempo.

### **Cenário:**

Vamos utilizar uma tabela de **clientes** e simular mudanças em algumas informações, como a **cidade** do cliente. Implementaremos o SCD Tipo 2, armazenando cada versão das mudanças em uma tabela Delta, para que possamos visualizar a evolução das dimensões de clientes ao longo do tempo.

### **Explicação do Exemplo 11:**

1. **Carga Inicial de Clientes**: No início, carregamos uma lista de clientes e suas respectivas cidades, todos marcados como registros ativos.
2. **Novas Alterações em Clientes**: Simulamos uma mudança nos dados de alguns clientes, onde o cliente **Carlos** se muda para **Rio de Janeiro** e o cliente **Rich** se muda para **Campinas**.
3. **SCD Tipo 2**: O processo de SCD Tipo 2 é implementado pela função `aplicar_scd2()`, que:
    - Marca o registro antigo como inativo (definindo o campo `fim_validade`).
    - Insere um novo registro para o cliente com as alterações recentes, mantendo o histórico.
4. **Histórico de Versões**: Cada alteração cria uma nova versão do registro no Delta Lake, preservando o histórico e garantindo que o estado anterior dos dados seja mantido.

### **Resultado Final:**

No final do processo, o Delta Lake contém múltiplas versões dos clientes, uma versão ativa e outras inativas, representando as diferentes cidades em que os clientes estiveram. Isso é essencial para manter um registro completo e histórico das mudanças, algo crítico para auditorias, relatórios históricos e análise de dados temporal.

### **Conclusão:**

Neste exemplo, vimos como o SCD Tipo 2 pode ser implementado usando o Delta Lake, aproveitando suas funcionalidades de versionamento e merge. Esse é um exemplo clássico de como gerenciar dados históricos de maneira eficiente e robusta em ambientes de Data Warehousing.