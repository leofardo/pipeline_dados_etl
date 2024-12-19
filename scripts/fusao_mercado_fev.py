from processamento_dados import Dados, Pipeline

#Extração

path_json = './data_raw/dados_empresaA.json'
path_csv = './data_raw/dados_empresaB.csv'

dados_empresaA = Dados(path_json, 'json')
dados_json = dados_empresaA.leitura_arquivos()

dados_empresaB = Dados(path_csv, 'csv')
dados_csv = dados_empresaB.leitura_arquivos()

# #Transformação

# De Para
key_mapping = {
    'Nome do Item': 'Nome do Produto',
    'Classificação do Produto': 'Categoria do Produto',
    'Valor em Reais (R$)': 'Preço do Produto (R$)',
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda': 'Data da Venda'
}

# #Trocando os nomes das colunas do CSV mantendo o mesmo padrão de nomes das colunas JSON
dados_empresaB.rename_columns(key_mapping)

#Combinando os dados
dados_combinados_tabela = Pipeline(dados_empresaA.dados, dados_empresaB.dados)

#Carregamento
path_dados_combinados = './data_processed/dados_combinados.csv'
dados_combinados_tabela.salvar_arquivo(path_dados_combinados)









