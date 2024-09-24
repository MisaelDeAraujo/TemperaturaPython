import pandas as pd
import numpy as np
import datetime
from azure.storage.blob import BlobServiceClient


class MonitoramentoTemperatura:
    def __init__(self, num_dados, conexao_string, container):
        self.num_dados = num_dados
        self.conexao_string = conexao_string
        self.container = container

    def gerar_dados_temperatura(self):
        temperaturas = []
        for _ in range(self.num_dados):
            temperatura = round(np.random.uniform(15, 30), 2)  # Temperatura entre 15 e 30 graus
            timestamp = datetime.datetime.now().isoformat()
            temperaturas.append({'timestamp': timestamp, 'temperatura': temperatura})
        return temperaturas

    def salvar_em_csv(self, dados):
        df = pd.DataFrame(dados)
        df.to_csv('temperaturas.csv', index=False)
        print("Dados de temperatura salvos em 'temperaturas.csv'.")

    def enviar_para_azure(self, nome_blob):
        blob_service_client = BlobServiceClient.from_connection_string(self.conexao_string)
        blob_client = blob_service_client.get_blob_client(container=self.container, blob=nome_blob)

        with open('temperaturas.csv', "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Arquivo '{nome_blob}' enviado para o Azure.")


if __name__ == "__main__":
    # Configurações
    num_dados = 10  # Número de dados a serem coletados
    conexao_string = 'chave_conexão'  # Substitua pela sua string de conexão
    container = 'dadosprojeto'  # Substitua pelo nome do seu container

    monitoramento = MonitoramentoTemperatura(num_dados, conexao_string, container)

    dados_temperatura = monitoramento.gerar_dados_temperatura()
    monitoramento.salvar_em_csv(dados_temperatura)
    monitoramento.enviar_para_azure('temperaturas.csv')
