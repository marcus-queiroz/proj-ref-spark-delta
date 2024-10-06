import json

# 1. Carregar arquivo JSON
def carregar_json(arquivo):
    with open(arquivo, 'r') as file:
        return json.load(file)

# 2. Salvar arquivo JSON
def salvar_json(dados, arquivo):
    with open(arquivo, 'w') as file:
        json.dump(dados, file, indent=4)

# 3. Exibir dados
def exibir_dados(dados):
    print(json.dumps(dados, indent=4))

# 4. Acessar dados do JSON
def acessar_dados(dados):
    nome = dados.get("pessoa", {}).get("nome", "N/A")
    habilidades = dados.get("pessoa", {}).get("habilidades", [])
    print(f'Nome: {nome}')
    print(f'Habilidades: {", ".join(habilidades)}')

# 5. Atualizar e modificar dados
def atualizar_dados(dados):
    # Atualizar idade
    if "pessoa" in dados:
        dados["pessoa"]["idade"] = 31
        # Adicionar cidade
        dados["pessoa"]["cidade"] = "São Paulo"

# 6. Manipular listas e dicionários no JSON
def manipular_listas(dados):
    if "pessoa" in dados:
        # Adicionar nova habilidade
        dados['pessoa']['habilidades'].append('C#')
        # Remover uma habilidade
        if 'SQL' in dados['pessoa']['habilidades']:
            dados['pessoa']['habilidades'].remove('SQL')

# 7. Validar JSON
def validar_json(json_str):
    try:
        dados = json.loads(json_str)
        print("JSON válido")
        return dados
    except json.JSONDecodeError as e:
        print(f"Erro no JSON: {e}")
        return None

# 8. Combinar dois arquivos JSON
def combinar_json(arquivo1, arquivo2):
    dados1 = carregar_json(arquivo1)
    dados2 = carregar_json(arquivo2)
    return {**dados1, **dados2}

# 9. Função principal para rodar o script
def main():
    # Carregar JSON
    dados = carregar_json('dados.json')

    # Exibir dados iniciais
    print("Dados Iniciais:")
    exibir_dados(dados)

    # Acessar dados
    acessar_dados(dados)

    # Atualizar dados
    atualizar_dados(dados)

    # Manipular listas
    manipular_listas(dados)

    # Exibir dados após modificações
    print("\nDados Após Modificações:")
    exibir_dados(dados)

    # Salvar dados modificados
    salvar_json(dados, 'dados_modificados.json')

    # Validar um JSON de exemplo
    json_str = '{"nome": "Rich", "idade": "trinta"}'
    validar_json(json_str)

    # Combinar dois arquivos JSON
    dados_combinados = combinar_json('dados1.json', 'dados2.json')
    print("\nDados Combinados:")
    exibir_dados(dados_combinados)

if __name__ == '__main__':
    main()
