import json

# Carregar dados de transações e clientes a partir dos arquivos JSON simulados
transacoes_json = {
    "transacoes": [
        {"id": 1, "cliente_id": 101, "valor": 1000, "data": "2024-01-15", "categoria": "Eletrônicos"},
        {"id": 2, "cliente_id": 102, "valor": 500, "data": "2024-01-16", "categoria": "Roupas"}
    ]
}

clientes_json = {
    "clientes": [
        {"cliente_id": 101, "nome": "Rich", "idade": 30, "cidade": "São Paulo"},
        {"cliente_id": 102, "nome": "Ana", "idade": 28, "cidade": "Rio de Janeiro"},
        {"cliente_id": 103, "nome": "Carlos", "idade": 35, "cidade": "Belo Horizonte"}
    ]
}

# 1. Carregar transações e clientes
transacoes = transacoes_json["transacoes"]
clientes = clientes_json["clientes"]

# 2. Integrar os dados, unindo transações com informações dos clientes
dados_integrados = []
for transacao in transacoes:
    cliente_info = next((cliente for cliente in clientes if cliente["cliente_id"] == transacao["cliente_id"]), None)
    if cliente_info:
        transacao_completa = {**transacao, **cliente_info}  # Combina os dois dicionários
        dados_integrados.append(transacao_completa)

# Exibir os dados integrados
print("Dados Integrados (Transações + Clientes):")
for dado in dados_integrados:
    print(dado)

# 3. Limpar dados duplicados (por exemplo, se tivermos clientes duplicados)
cliente_ids_vistos = set()
dados_limpos = []
for dado in dados_integrados:
    if dado["cliente_id"] not in cliente_ids_vistos:
        dados_limpos.append(dado)
        cliente_ids_vistos.add(dado["cliente_id"])

# Exibir dados após a limpeza
print("\nDados Limpos (Sem duplicados):")
for dado in dados_limpos:
    print(dado)
