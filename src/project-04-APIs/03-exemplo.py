import json
from collections import defaultdict

# Carregar JSON de transações
dados = {
    "transacoes": [
        {"id": 1, "cliente": "Rich", "valor": 1000, "data": "2024-01-15", "categoria": "Eletrônicos"},
        {"id": 2, "cliente": "Ana", "valor": 500, "data": "2024-01-16", "categoria": "Roupas"},
        {"id": 3, "cliente": "Carlos", "valor": 2000, "data": "2024-02-01", "categoria": "Eletrônicos"},
        {"id": 4, "cliente": "Rich", "valor": 300, "data": "2024-02-10", "categoria": "Alimentos"},
        {"id": 5, "cliente": "Ana", "valor": 700, "data": "2024-03-01", "categoria": "Eletrônicos"}
    ]
}

# 1. Filtrar transações com valor acima de 1000
transacoes_acima_de_1000 = [t for t in dados["transacoes"] if t["valor"] > 1000]
print("Transações acima de 1000:")
for transacao in transacoes_acima_de_1000:
    print(transacao)

# 2. Somar valor das transações por categoria
totais_por_categoria = defaultdict(float)
for transacao in dados["transacoes"]:
    categoria = transacao["categoria"]
    valor = transacao["valor"]
    totais_por_categoria[categoria] += valor

print("\nTotais por categoria:")
for categoria, total in totais_por_categoria.items():
    print(f"{categoria}: {total}")

# 3. Agrupar transações por cliente e calcular o total gasto
totais_por_cliente = defaultdict(float)
for transacao in dados["transacoes"]:
    cliente = transacao["cliente"]
    valor = transacao["valor"]
    totais_por_cliente[cliente] += valor

print("\nTotais por cliente:")
for cliente, total in totais_por_cliente.items():
    print(f"{cliente}: {total}")
