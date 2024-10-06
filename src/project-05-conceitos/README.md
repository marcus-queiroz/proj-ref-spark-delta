### **Indicadores

Para este projeto corporativo que combina análise de vendas e gestão de estoques, definimos alguns indicadores principais que nos ajudarão a extrair insights e apoiar a tomada de decisões no negócio.

#### **Indicadores de Vendas**

1. **Total de Vendas por Cliente**:
    
    - **Objetivo**: Medir quanto cada cliente gastou no e-commerce.
    - **Dados necessários**:
        - **Pedidos (orders)**: `customer_id`, `total_amount`, `order_date`.
        - **Clientes (customers)**: `customer_id`, `customer_name`, `customer_address`.
    - **Indicadores derivados**:
        - Total gasto por cliente (soma de `total_amount` agrupado por `customer_id`).
        - Quantidade de pedidos por cliente.
2. **Vendas por Categoria de Produto**:
    
    - **Objetivo**: Identificar quais categorias de produto têm o maior volume de vendas.
    - **Dados necessários**:
        - **Pedidos (orders)**: `product_id`, `total_amount`.
        - **Produtos (products)**: `product_id`, `category`.
    - **Indicadores derivados**:
        - Vendas totais por categoria (soma de `total_amount` agrupado por `category`).
        - Quantidade de produtos vendidos por categoria.
3. **Taxa de Conversão de Pedidos**:
    
    - **Objetivo**: Monitorar quantos pedidos foram completados em comparação com pedidos cancelados ou incompletos.
    - **Dados necessários**:
        - **Pedidos (orders)**: `order_status`, `order_date`.
    - **Indicadores derivados**:
        - Taxa de conversão (divisão entre pedidos completados e pedidos totais).

#### **Indicadores de Estoque**

1. **Nível de Estoque por Produto**:
    
    - **Objetivo**: Manter controle sobre a quantidade disponível de cada produto no estoque.
    - **Dados necessários**:
        - **Movimentação de Estoque (inventory_movements)**: `product_id`, `movement_type`, `quantity`, `movement_date`.
        - **Produtos (products)**: `product_id`, `product_name`.
    - **Indicadores derivados**:
        - Estoque disponível por produto (somatório de `quantity` agrupado por `product_id`).
2. **Previsão de Demanda e Ajustes de Estoque**:
    
    - **Objetivo**: Prever a demanda futura de produtos e ajustar os níveis de estoque com base nas vendas passadas e movimentações de estoque.
    - **Dados necessários**:
        - **Previsão de Vendas (forecasted_sales)**: `product_id`, `forecasted_quantity`, `forecast_date`.
        - **Pedidos (orders)**: `product_id`, `total_amount`, `order_date`.
    - **Indicadores derivados**:
        - Diferença entre o estoque disponível e a previsão de vendas (para ajustes de estoque).
        - Produtos com maior risco de ruptura (produtos com estoque disponível menor que a previsão de demanda).
3. **Custo de Manutenção de Estoque**:
    
    - **Objetivo**: Monitorar o custo associado à manutenção de estoque, considerando a quantidade de produtos armazenados ao longo do tempo.
    - **Dados necessários**:
        - **Movimentação de Estoque (inventory_movements)**: `product_id`, `quantity`, `movement_date`.
        - **Produtos (products)**: `product_id`, `storage_cost_per_unit`.
    - **Indicadores derivados**:
        - Custo total de manutenção de estoque por produto (multiplicação de `quantity` pelo `storage_cost_per_unit`).

---

### **Geração das Bases de Dados**

Abaixo estão as bases de dados que iremos gerar para fornecer as informações necessárias para os indicadores mencionados:

#### 1. **Pedidos (orders)**

**Descrição**: Contém informações sobre cada pedido realizado, incluindo detalhes sobre o cliente e o valor total do pedido.

- **Colunas**:
    - `order_id`: Identificador único do pedido.
    - `customer_id`: Identificador do cliente que fez o pedido.
    - `product_id`: Identificador do produto pedido.
    - `total_amount`: Valor total do pedido.
    - `order_status`: Status do pedido (completado, cancelado, em processamento).
    - `order_date`: Data em que o pedido foi realizado.

#### 2. **Clientes (customers)**

**Descrição**: Detalhes dos clientes, como nome, endereço e informações de contato.

- **Colunas**:
    - `customer_id`: Identificador único do cliente.
    - `customer_name`: Nome do cliente.
    - `customer_address`: Endereço do cliente.
    - `email`: Email do cliente.
    - `registration_date`: Data de cadastro do cliente.

#### 3. **Movimentação de Estoques (inventory_movements)**

**Descrição**: Registra todas as movimentações de estoque, tanto entradas quanto saídas, para cada produto.

- **Colunas**:
    - `movement_id`: Identificador único da movimentação.
    - `product_id`: Identificador do produto.
    - `movement_type`: Tipo de movimentação (entrada ou saída).
    - `quantity`: Quantidade movimentada.
    - `movement_date`: Data da movimentação.

#### 4. **Produtos (products)**

**Descrição**: Contém detalhes sobre os produtos, como nome, categoria e custo de armazenamento.

- **Colunas**:
    - `product_id`: Identificador único do produto.
    - `product_name`: Nome do produto.
    - `category`: Categoria do produto.
    - `price`: Preço unitário do produto.
    - `storage_cost_per_unit`: Custo de armazenamento por unidade.

#### 5. **Previsão de Vendas (forecasted_sales)**

**Descrição**: Contém previsões de vendas para cada produto, com base em dados históricos de movimentação de estoque e pedidos.

- **Colunas**:
    - `product_id`: Identificador do produto.
    - `forecasted_quantity`: Quantidade prevista para vendas futuras.
    - `forecast_date`: Data da previsão.

---

### **Dicionário de Dados**
```markdown
| Tabela               | Coluna                 | Descrição                                          | Tipo    |
|----------------------|------------------------|----------------------------------------------------|---------|
| orders               | order_id               | Identificador único do pedido                      | INT     |
|                      | customer_id            | Identificador do cliente que fez o pedido          | INT     |
|                      | product_id             | Identificador do produto pedido                    | INT     |
|                      | total_amount           | Valor total do pedido                              | DECIMAL |
|                      | order_status           | Status do pedido (completado, cancelado, etc.)     | STRING  |
|                      | order_date             | Data do pedido                                     | DATE    |
| customers            | customer_id            | Identificador único do cliente                     | INT     |
|                      | customer_name          | Nome do cliente                                    | STRING  |
|                      | customer_address       | Endereço do cliente                                | STRING  |
|                      | email                  | Email do cliente                                   | STRING  |
|                      | registration_date      | Data de registro do cliente                        | DATE    |
| inventory_movements  | movement_id            | Identificador único da movimentação                | INT     |
|                      | product_id             | Identificador do produto                           | INT     |
|                      | movement_type          | Tipo de movimentação (entrada ou saída)            | STRING  |
|                      | quantity               | Quantidade movimentada                             | DECIMAL |
|                      | movement_date          | Data da movimentação                               | DATE    |
| products             | product_id             | Identificador único do produto                     | INT     |
|                      | product_name           | Nome do produto                                    | STRING  |
|                      | category               | Categoria do produto                               | STRING  |
|                      | price                  | Preço unitário do produto                          | DECIMAL |
|                      | storage_cost_per_unit  | Custo de armazenamento por unidade do produto      | DECIMAL |
| forecasted_sales     | product_id             | Identificador do produto                           | INT     |
|                      | forecasted_quantity    | Quantidade prevista de vendas futuras              | DECIMAL |
|                      | forecast_date          | Data da previsão                                   | DATE    |
```


### **Relacionamento entre os Dados**

1. **Pedidos (`orders`)** e **Clientes (`customers`)**:
    
    - **Relação 1
        
        **: Um cliente pode realizar vários pedidos, mas cada pedido pertence a um único cliente. Essa relação será usada para a implementação do **SCD Tipo 2**, rastreando mudanças nos dados dos clientes, como endereço ou email.
2. **Pedidos (`orders`)** e **Movimentação de Estoques (`inventory_movements`)**:
    
    - **Relação indireta**: Um pedido gerará uma movimentação de saída no estoque. Cada vez que um pedido é completado, o estoque do produto correspondente é reduzido.
3. **Produtos (`products`)** e **Movimentação de Estoques (`inventory_movements`)**:
    
    - **Relação 1
        
        **: Cada produto pode ter múltiplas movimentações de estoque (entradas e saídas). A movimentação é registrada toda vez que o estoque é alterado, seja por compras ou ajustes.
4. **Produtos (`products`)** e **Previsão de Vendas (`forecasted_sales`)**:
    
    - **Relação 1
        
        **: Cada produto possui várias previsões de vendas ao longo do tempo. Usamos esses dados para ajustar o nível de estoque e prever a demanda futura.

Agora que os dados estão definidos, posso gerar os scripts para cada uma das camadas (Bronze, Silver, Gold), garantindo a implementação dos requisitos e indicadores abordados.

