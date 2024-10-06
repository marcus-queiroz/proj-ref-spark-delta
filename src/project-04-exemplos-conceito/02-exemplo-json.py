import json
"""
Acessa dados aninhados: Extrai o nome da empresa e a cidade onde está localizada.
Itera sobre listas de dicionários: Lista os funcionários e suas habilidades.
Atualiza dados: Modifica o status do projeto "Datalake" de "Rich" para "Concluído".
Adiciona novos dados: Insere um novo funcionário na lista de funcionários.
Remove dados: Remove o projeto "Relatórios Mensais" da lista de projetos de "Ana".
Valida a estrutura: Verifica se todos os funcionários têm o campo "habilidades".
Serializa e desserializa JSON: Converte o JSON para string, modifica e o converte de volta para JSON.
"""


# JSON Complexo (Definido em Python para simplificação)
dados = {
    "empresa": {
        "nome": "Tech Solutions",
        "localizacao": {
            "pais": "Brasil",
            "cidade": "São Paulo",
            "endereco": {
                "rua": "Rua Principal",
                "numero": 100
            }
        },
        "funcionarios": [
            {
                "nome": "Rich",
                "cargo": "Engenheiro de Dados",
                "idade": 30,
                "habilidades": ["Python", "SQL", "Power BI"],
                "projetos": [
                    {"nome": "Datalake", "status": "Em andamento"},
                    {"nome": "Dashboards", "status": "Concluído"}
                ]
            },
            {
                "nome": "Ana",
                "cargo": "Analista de BI",
                "idade": 28,
                "habilidades": ["Power BI", "SQL"],
                "projetos": [
                    {"nome": "Relatórios Mensais", "status": "Em andamento"}
                ]
            }
        ]
    }
}

# 1. Acessando dados aninhados
nome_empresa = dados["empresa"]["nome"]
cidade = dados["empresa"]["localizacao"]["cidade"]
print(f"Empresa: {nome_empresa}, Cidade: {cidade}\n")

# 2. Iterando sobre lista de dicionários (funcionários e habilidades)
print("Funcionários e suas habilidades:")
for funcionario in dados["empresa"]["funcionarios"]:
    nome = funcionario["nome"]
    habilidades = ", ".join(funcionario["habilidades"])
    print(f"Funcionário: {nome}, Habilidades: {habilidades}")

print("\n")

# 3. Atualizando o status do projeto "Datalake" para "Concluído"
for funcionario in dados["empresa"]["funcionarios"]:
    if funcionario["nome"] == "Rich":
        for projeto in funcionario["projetos"]:
            if projeto["nome"] == "Datalake":
                projeto["status"] = "Concluído"

# Verificando a atualização
print("Projetos de Rich após atualização:")
for funcionario in dados["empresa"]["funcionarios"]:
    if funcionario["nome"] == "Rich":
        print(funcionario["projetos"])

print("\n")

# 4. Adicionando um novo funcionário
novo_funcionario = {
    "nome": "Carlos",
    "cargo": "Cientista de Dados",
    "idade": 35,
    "habilidades": ["Python", "Machine Learning", "SQL"],
    "projetos": [
        {"nome": "Previsão de Vendas", "status": "Iniciado"}
    ]
}
dados["empresa"]["funcionarios"].append(novo_funcionario)

# Verificando se o novo funcionário foi adicionado
print("Lista de funcionários após adicionar Carlos:")
for funcionario in dados["empresa"]["funcionarios"]:
    print(f"Funcionário: {funcionario['nome']}, Cargo: {funcionario['cargo']}")

print("\n")

# 5. Removendo o projeto "Relatórios Mensais" da Ana
for funcionario in dados["empresa"]["funcionarios"]:
    if funcionario["nome"] == "Ana":
        funcionario["projetos"] = [projeto for projeto in funcionario["projetos"] if projeto["nome"] != "Relatórios Mensais"]

# Verificando se o projeto foi removido
print("Projetos de Ana após remoção:")
for funcionario in dados["empresa"]["funcionarios"]:
    if funcionario["nome"] == "Ana":
        print(funcionario["projetos"])

print("\n")

# 6. Validando a estrutura do JSON (verifica se há campo "habilidades" para todos os funcionários)
def validar_json_complexo(dados):
    for funcionario in dados["empresa"]["funcionarios"]:
        if "habilidades" not in funcionario:
            print(f"O funcionário {funcionario['nome']} está sem o campo 'habilidades'.")

# Validação do JSON
validar_json_complexo(dados)

print("\n")

# 7. Serializando o JSON para string e modificando
json_str = json.dumps(dados, indent=4)
print("JSON como string:\n", json_str)

# Modificar a string convertendo de volta para objeto Python
dados_modificados = json.loads(json_str)
dados_modificados["empresa"]["localizacao"]["pais"] = "Argentina"

print("\nJSON modificado:\n", json.dumps(dados_modificados, indent=4))
