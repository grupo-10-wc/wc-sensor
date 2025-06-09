import os
import io
import csv
import pdb
import time
import json
import pandas as pd
import matplotlib.pyplot as plt
import boto3

from typing import Callable 
from database import Database
from dotenv import load_dotenv
from memory_profiler import memory_usage
from simulador_sensores import SimuladorSensor
from azure.iot.device import IoTHubDeviceClient, Message
from azure.storage.blob import BlobServiceClient


load_dotenv()


class AlgasBenchmark:
    def __init__(self):
        # Inicializa o banco de dados
        os.mkdir('output') if not os.path.exists('output') else None
        os.mkdir('output/plot') if not os.path.exists('output/plot') else None
        os.mkdir('output/csv') if not os.path.exists('output/csv') else None
        self.db = Database(db_name="algas")
        self.db.create_table()
        self.sensores_table = self.db.sensores()
        self.teste_carga_table = self.db.teste_carga()
        self.connection_string = os.getenv("CONNECTION_STRING")
        self.simulador = SimuladorSensor(self.db, n_dados=500, intervalo_ms=1000*60*30, alerta="nenhum")
        

    def open_iot_hub_connection(self):
        client = IoTHubDeviceClient.create_from_connection_string(self.connection_string)
        client.connect()
        #client.get_storage_info_for_blob()
        return client

    def send_to_blob(self, client:IoTHubDeviceClient, records:list[dict]):
        df = pd.DataFrame(records)
        df['created_at'] = df['created_at'].dt.strftime("%Y-%m-%d %H:%M:%S")
        json_str = json.dumps(df.to_dict('records'), indent=2)
        blob_name = "blobiothub02231023"
        storage_info = client.get_storage_info_for_blob(blob_name)
        
        
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_info.get('hostName', 'your-storage-account.blob.core.windows.net')}"
        )
        
        blob_client = blob_service_client.get_blob_client(
            container=storage_info.get('containerName', 'rawdata'),
            blob=blob_name
        )
        
        # Upload do arquivo
        with open("output/data.parquet", 'rb') as data:
            blob_client.upload_blob(data, overwrite=True)
        
        # Notifica o IoT Hub sobre o upload
        
        client.disconnect()
        
        print(f"Arquivo {blob_name} enviado com sucesso para o Blob Storage!")
        
    def send_iot_hub_message(self, client:IoTHubDeviceClient, records:list[dict]):
        df = pd.DataFrame(records)
        df['created_at'] = df['created_at'].dt.strftime("%Y-%m-%d %H:%M:%S")
        json_str = json.dumps(df.to_dict('records'), indent=2)
        print("\n\n\nenviado\n\n\n")
        client.send_message(Message(json_str)) 


    def gerar_grafico_dados_sensores(self, sensor_data, sensor_model, cenario, intervalo_agrupamento=10):
        """
        Gera um gráfico com os dados simulados de um sensor, aplicando suavização e agrupamento.

        :param sensor_data: Lista de dicionários com os dados do sensor.
        :param sensor_model: Nome do modelo do sensor.
        :param cenario: Identificação do cenário.
        :param intervalo_agrupamento: Número de pontos a serem agrupados para suavização.
        """
        # Extrai os dados para o gráfico
        if sensor_model == "fluke_1735":
            df = pd.DataFrame(sensor_data)
            plt.figure(figsize=(12, 6))
            plt.plot(df['created_at'], df['data'], 'b-', linewidth=2, label='Fator de Potência')
            plt.axhline(y=0.92, color='r', linestyle='--', label='Fator de Potência de Referência (0.92)')
            plt.grid(True)
            plt.xlabel('Data/Hora')
            plt.ylabel('Fator de Potência')
            plt.title('Simulação do Fator de Potência com Variações Aleatórias')
            plt.legend()
            plt.ylim(0.70, .98)
            grafico_filename = f'grafico_{sensor_model.lower().replace(" ", "_")}_cenario_{cenario}.png'
            plt.savefig(f'output/plot/{grafico_filename}')
        
        elif sensor_model == "hms_m21":
            timestamps = [record['created_at'] for record in sensor_data]
            valores = [record['data'] for record in sensor_data]

            agrupados_timestamps = []
            agrupados_valores = []

            for i in range(0, len(valores), intervalo_agrupamento):
                agrupados_timestamps.append(timestamps[i])
                agrupados_valores.append(sum(valores[i:i + intervalo_agrupamento]) / len(valores[i:i + intervalo_agrupamento]))

            
            plt.figure(figsize=(10, 6))
            plt.plot(agrupados_timestamps, agrupados_valores, label=f'{sensor_model}', color='blue', marker='o', markersize=4)

            plt.axhline(y=40, color='red', linestyle='-', linewidth=1.5, label='Temperatura Máxima (40°C)')
            plt.axhline(y=-5, color='red', linestyle='-', linewidth=1.5, label='Temperatura Mínima (-5°C)')

            plt.axhline(y=30, color='orange', linestyle='--', linewidth=1.5, label='Temperatura Alta (30°C)')
            plt.axhline(y=5, color='orange', linestyle='--', linewidth=1.5, label='Temperatura Baixa (5°C)')

            plt.title(f'Dados do Sensor {sensor_model} - Cenário {cenario} - Quadro de Energia')
            plt.xlabel('Data e Hora')
            plt.ylabel('Valor')
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.legend(loc='lower right')

            grafico_filename = f'grafico_{sensor_model.lower().replace(" ", "_")}_cenario_{cenario}.png'
            plt.savefig(f'output/plot/{grafico_filename}', dpi=300, bbox_inches='tight')

        elif sensor_model == "ct_clamp":
            timestamps = [record['created_at'] for record in sensor_data]
            valores = [record['data'] for record in sensor_data]

            agrupados_timestamps = []
            agrupados_valores = []

            for i in range(0, len(valores), intervalo_agrupamento):
                agrupados_timestamps.append(timestamps[i])
                agrupados_valores.append(sum(valores[i:i + intervalo_agrupamento]) / len(valores[i:i + intervalo_agrupamento]))

            
            plt.figure(figsize=(10, 6))
            plt.plot(agrupados_timestamps, agrupados_valores, label=f'{sensor_model}', color='blue', marker='o', markersize=4)

            plt.axhline(y=10, color='red', linestyle='-', linewidth=1.5, label='Corrente máxima')

            plt.axhline(y=-2, color='orange', linestyle='--', linewidth=1.5, label='Corrente mínima')

            plt.title(f'Dados do Sensor {sensor_model} - Cenário {cenario} - Quadro de Energia')
            plt.xlabel('Data e Hora')
            plt.ylabel('Valor')
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.legend(loc='lower right')

            grafico_filename = f'grafico_{sensor_model.lower().replace(" ", "_")}_cenario_{cenario}.png'
            plt.savefig(f'output/plot/{grafico_filename}', dpi=300, bbox_inches='tight')

        else:
            timestamps = [record['created_at'] for record in sensor_data]
            valores = [record['data'] for record in sensor_data]

            # Agrupamento e suavização
            agrupados_timestamps = []
            agrupados_valores = []

            for i in range(0, len(valores), intervalo_agrupamento):
                agrupados_timestamps.append(timestamps[i])
                agrupados_valores.append(sum(valores[i:i + intervalo_agrupamento]) / len(valores[i:i + intervalo_agrupamento]))

            # Configura o gráfico
            plt.figure(figsize=(10, 6))
            plt.plot(agrupados_timestamps, agrupados_valores, label=f'{sensor_model}', color='blue', marker='o', markersize=4)
            plt.title(f'Dados do Sensor {sensor_model} - Cenário {cenario}')
            plt.xlabel('Data e Hora')
            plt.ylabel('Valor')
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.legend()

            # Salva o gráfico como arquivo PNG
            grafico_filename = f'grafico_{sensor_model.lower().replace(" ", "_")}_cenario_{cenario}.png'
            plt.savefig(f'output/plot/{grafico_filename}', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"Gráfico gerado: {grafico_filename}")


    def benchmark_sensor(self, sensor_func: Callable, cenario: int):
        """
        Executa o benchmark para uma função de simulação de sensor.
 
        :param sensor_func: Função de simulação de sensor a ser testada.
        :param cenario: Identificação do cenário.
        :return: DataFrame com os resultados do benchmark.
        """
        blocos = []
        tempo = []
        memoria = []

        # csv_filename = f"output/csv/cenario_{sensor_func.__name__}_benchmark.csv"
        sensor_csv_filename = f"output/csv/cenario_{sensor_func.__name__}_sensores.csv"

        # open(csv_filename, 'w', newline='') as csvfile, 
        with open(sensor_csv_filename, 'w', newline='') as sensor_csvfile:
            # benchmark_writer = csv.writer(csvfile)
            # benchmark_writer.writerow(['tamanho_bloco', 'duracao', 'memoria_mb'])

            sensor_writer = csv.writer(sensor_csvfile)
            sensor_writer.writerow(['sensor_model', 'measure_unit', 'device', 'location', 'data_type', 'data', 'created_at'])

            for block_size in range(1):  # Ajuste o intervalo conforme necessário
                print(f"Iniciando simulação para {block_size} dados...")

                # Atualiza o número de dados no simulador
                # self.simulador.n_dados = block_size

                def processar_sensor():
                    start_time = time.time()
                    dados = sensor_func()
                    end_time = time.time()
                    return dados, end_time - start_time

                mem_usage, (dados, duration) = memory_usage((processar_sensor, ()), max_usage=True, retval=True)
                duration = float(f"{duration:.2f}")

                # blocos.append(block_size)
                # tempo.append(duration)
                # memoria.append(mem_usage)

                # Salva os dados dos sensores no CSV
                for record in dados:
                    sensor_writer.writerow(record.values())

                # Salva os resultados do benchmark no CSV
                # benchmark_writer.writerow([block_size, duration, mem_usage])

                print(f"Simulação para {block_size} dados concluída em {duration} segundos.")
                # print(f"Uso de memória: {mem_usage:.2f} MB\n")

        # Envia o CSV dos sensores para o Azure Service Bus
        # service_bus_connection_string = 'COLOCAR AQUI'
        # queue_name = "dados-sensores"

        # Gera o gráfico com os dados dos sensores

        # self.gerar_grafico_dados_sensores(dados, sensor_func.__name__, cenario)

        # Cria um DataFrame com os resultados do benchmark
        df_results = pd.DataFrame({
            'tempo': tempo,
            'blocos': blocos,
            'memoria': memoria,
            'cenario': cenario
        })

        return df_results, dados

    def enviar_csv_para_s3(self, bucket_name='terraform-20250429234641902400000001', prefixo='csv/'):        
        s3 = boto3.client('s3')
        folder_path = 'output/csv'

        if not os.path.exists(folder_path):
            print(f"Pasta {folder_path} não existe.")
            return

        for filename in os.listdir(folder_path):
            if filename.endswith(".csv"):
                local_path = os.path.join(folder_path, filename)
                s3_key = f"{prefixo}{filename}"
                try:
                    s3.upload_file(local_path, bucket_name, s3_key)
                    print(f"Arquivo enviado para S3: {s3_key}")
                except Exception as e:
                    print(f"Erro ao enviar {filename} para S3: {e}")


    def run(self):
        cenarios = [
            {"cenario": 1, "sensor_func": self.simulador.shelly_em},
            {"cenario": 2, "sensor_func": self.simulador.sonoff_pow_r3},
            {"cenario": 3, "sensor_func": self.simulador.pzem_004t},
            {"cenario": 4, "sensor_func": self.simulador.fluke_1735},
            {"cenario": 5, "sensor_func": self.simulador.hms_m21},
            {"cenario": 6, "sensor_func": self.simulador.ct_clamp}
        ]

        # fig_tempo_blocos, ax_tempo_blocos = plt.subplots(figsize=(10, 6))
        # fig_mem_blocos, ax_mem_blocos = plt.subplots(figsize=(10, 6))

        # ax_tempo_blocos.grid(True, linestyle='--', alpha=0.7)
        # ax_mem_blocos.grid(True, linestyle='--', alpha=0.7)

        # ax_tempo_blocos.set_yscale('log')
        # ax_mem_blocos.set_yscale('log')

        # df_benchmark = pd.DataFrame()
        df_dados = pd.DataFrame()

        for cenario in cenarios:
            print(f"Executando benchmark para o cenário {cenario['cenario']}...")
            df_cenario, dados = self.benchmark_sensor(cenario["sensor_func"], cenario["cenario"])
            df_dados = pd.concat([df_dados, pd.DataFrame(dados)])
            # df_benchmark = pd.concat([df_benchmark, df_cenario], ignore_index=True)

            # ax_tempo_blocos.plot(df_cenario['tempo'], df_cenario['blocos'], label=f'Cenário {cenario["cenario"]}', marker='o')
            # ax_mem_blocos.plot(df_cenario['memoria'], df_cenario['blocos'], label=f'Cenário {cenario["cenario"]}', marker='o')

        # Salva os resultados do benchmark no banco de dados
        # self.db.db_execute(self.teste_carga_table.insert().values(df_benchmark.to_dict("records")), commit=True)
        df_dados.to_csv("output/csv/dados.csv", index=False)
        try:
            client = self.open_iot_hub_connection()
            self.send_iot_hub_message(client, df_dados.to_dict("records"))
            print("\033[32mDados enviados para o Azure com sucesso!\033[0m")
            client.disconnect()
        except ValueError as e:
            print(f"\033[31m{e} sem acesso a Azure !!!\033[0m")
        except Exception as e:
            print(f"Erro ao enviar dados para o Azure: {e}")
        # Configura os gráficos
        # ax_tempo_blocos.set_xlabel('Tempo de execução (segundos)')
        # ax_tempo_blocos.set_ylabel('Blocos processados')
        # ax_tempo_blocos.legend()

        # ax_mem_blocos.set_xlabel('Memória utilizada (MB)')
        # ax_mem_blocos.set_ylabel('Blocos processados')
        # ax_mem_blocos.legend()

        # # Salva os gráficos
        # fig_tempo_blocos.savefig('output/plot/benchmark_tempo.png', dpi=300, bbox_inches='tight')
        # fig_mem_blocos.savefig('output/plot/benchmark_memoria.png', dpi=300, bbox_inches='tight')

        # print("Benchmarks concluídos e gráficos salvos.")



if __name__ == "__main__":
    benchmark = AlgasBenchmark()
    benchmark.run()
    benchmark.enviar_csv_para_s3()