import json
import csv

class Dados:

    def __init__(self, path, tipo_dados):
        self.path = path
        self.tipo_dados = tipo_dados
        self.dados = self.leitura_arquivos()
        self.nomes_colunas = self.get_columns()

    def leitura_arquivos(self):
        with open(self.path, 'r') as file:
            return json.load(file) if self.tipo_dados == 'json' else list(csv.DictReader(file)) if self.tipo_dados == 'csv' else None
        
    def get_columns(self):
        return list(self.dados[1].keys())
    
    def rename_columns(self, key_mapping):
        new_dados = []

        for old_dict in self.dados:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_dados.append(dict_temp)

        self.dados = new_dados
        self.nomes_colunas = self.get_columns()

class Pipeline:

    def __init__(self, empresaA, empresaB):
        self.empresaA = empresaA
        self.empresaB = empresaB
        self.dados_unificados = self.combinar_arquivos()

    def combinar_arquivos(self):
        combined_list = []
        combined_list.extend(self.empresaA)
        combined_list.extend(self.empresaB)
        
        nome_colunas = []
        if len(combined_list[-1].keys()) > len(combined_list[0].keys()):
            nome_colunas = combined_list[-1].keys()
        else:
            nome_colunas = combined_list[0].keys()

        #criando a tabela apenas com o nome das colunas
        dados_combinados_tabela = [nome_colunas]

        for row in combined_list:
            linha = []
            for coluna in nome_colunas:
                #verificara se o valor da coluna existe, se nao exister inputará "Indisponível"
                linha.append(row.get(coluna, 'Indisponível'))
            dados_combinados_tabela.append(linha)
        return dados_combinados_tabela

    def salvar_arquivo(self, path):
        try:
            with open(path, 'w') as file:
                writer = csv.writer(file)
                writer.writerows(self.dados_unificados)
        except:
            print("Ocorreu um erro ao salvar o arquivo")
        else:
         print("Arquivo salvo com sucesso!")




        
