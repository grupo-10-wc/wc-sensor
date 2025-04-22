import sys
import csv
import time
import matplotlib.pyplot as plt
from memory_profiler import memory_usage
import numpy as np
import os
import pandas as pd
from simulador_sensores import SimuladorSensor
from database import Database
from azure.servicebus import ServiceBusClient, ServiceBusMessage

class AlgasBenchmark:
    def __init__(self):
        # Inicializa o banco de dados
        self.db = Database(db_name="algas")
        self.db.create_table()
        self.sensores_table = self.db.sensores()
        self.teste_carga_table = self.db.teste_carga()

        # Inicializa o simulador de sensores
        self.simulador = SimuladorSensor(self.db, n_dados=1000, intervalo_ms=60000, alerta="nenhum")

    def benchmark_sensor(self, sensor_func, cenario):
        """
        Executa o benchmark para uma função de simulação de sensor.

        :param sensor_func: Função de simulação de sensor a ser testada.
        :param cenario: Identificação do cenário.
        :return: DataFrame com os resultados do benchmark.
        """
        blocos = []
        tempo = []
        memoria = []

        csv_filename = f"cenario_{cenario}_benchmark.csv"
        sensor_csv_filename = f"cenario_{cenario}_sensores.csv"

        with open(csv_filename, 'w', newline='') as csvfile, open(sensor_csv_filename, 'w', newline='') as sensor_csvfile:
            benchmark_writer = csv.writer(csvfile)
            benchmark_writer.writerow(['tamanho_bloco', 'duracao', 'memoria_mb'])

            sensor_writer = csv.writer(sensor_csvfile)
            sensor_writer.writerow(['sensor_model', 'measure_unit', 'device', 'location', 'data_type', 'data', 'created_at'])

            for block_size in range(100, 1100, 200):  # Ajuste o intervalo conforme necessário
                print(f"Iniciando simulação para {block_size} dados...")

                # Atualiza o número de dados no simulador
                self.simulador.n_dados = block_size

                def processar_sensor():
                    start_time = time.time()
                    dados = sensor_func()
                    end_time = time.time()
                    return dados, end_time - start_time

                mem_usage, (dados, duration) = memory_usage((processar_sensor, ()), max_usage=True, retval=True)
                duration = float(f"{duration:.2f}")

                blocos.append(block_size)
                tempo.append(duration)
                memoria.append(mem_usage)

                # Salva os dados dos sensores no CSV
                for record in dados:
                    sensor_writer.writerow(record.values())

                # Salva os resultados do benchmark no CSV
                benchmark_writer.writerow([block_size, duration, mem_usage])

                print(f"Simulação para {block_size} dados concluída em {duration} segundos.")
                print(f"Uso de memória: {mem_usage:.2f} MB\n")

        # Envia o CSV dos sensores para o Azure Service Bus
        service_bus_connection_string = 'COLOCAR AQUI'
        queue_name = "dados-sensores"
        enviar_csv_para_service_bus(service_bus_connection_string, queue_name, sensor_csv_filename)

        # Gera o gráfico com os dados dos sensores
        gerar_grafico_dados_sensores(dados, sensor_func.__name__, cenario)

        # Cria um DataFrame com os resultados do benchmark
        df_results = pd.DataFrame({
            'tempo': tempo,
            'blocos': blocos,
            'memoria': memoria,
            'cenario': cenario
        })

        return df_results
    
    def main(self):
        cenarios = [
            {"cenario": 1, "sensor_func": self.simulador.shelly_em},
            {"cenario": 2, "sensor_func": self.simulador.sonoff_pow_r3},
            {"cenario": 3, "sensor_func": self.simulador.pzem_004t},
            {"cenario": 4, "sensor_func": self.simulador.fluke_1735},
        ]

        fig_tempo_blocos, ax_tempo_blocos = plt.subplots(figsize=(10, 6))
        fig_mem_blocos, ax_mem_blocos = plt.subplots(figsize=(10, 6))

        ax_tempo_blocos.grid(True, linestyle='--', alpha=0.7)
        ax_mem_blocos.grid(True, linestyle='--', alpha=0.7)

        ax_tempo_blocos.set_yscale('log')
        ax_mem_blocos.set_yscale('log')

        df = pd.DataFrame()

        for cenario in cenarios:
            print(f"Executando benchmark para o cenário {cenario['cenario']}...")
            df_cenario = self.benchmark_sensor(cenario["sensor_func"], cenario["cenario"])
            df = pd.concat([df, df_cenario], ignore_index=True)

            ax_tempo_blocos.plot(df_cenario['tempo'], df_cenario['blocos'], label=f'Cenário {cenario["cenario"]}', marker='o')
            ax_mem_blocos.plot(df_cenario['memoria'], df_cenario['blocos'], label=f'Cenário {cenario["cenario"]}', marker='o')

        # Salva os resultados do benchmark no banco de dados
        self.db.db_execute(self.teste_carga_table.insert().values(df.to_dict("records")), commit=True)

        # Configura os gráficos
        ax_tempo_blocos.set_xlabel('Tempo de execução (segundos)')
        ax_tempo_blocos.set_ylabel('Blocos processados')
        ax_tempo_blocos.legend()

        ax_mem_blocos.set_xlabel('Memória utilizada (MB)')
        ax_mem_blocos.set_ylabel('Blocos processados')
        ax_mem_blocos.legend()

        # Salva os gráficos
        fig_tempo_blocos.savefig('benchmark_tempo.png', dpi=300, bbox_inches='tight')
        fig_mem_blocos.savefig('benchmark_memoria.png', dpi=300, bbox_inches='tight')

        print("Benchmarks concluídos e gráficos salvos.")

def enviar_csv_para_service_bus(service_bus_connection_string, queue_name, csv_filename):
    """
    Envia um arquivo CSV como mensagem para o Azure Service Bus.

    :param service_bus_connection_string: Connection string do Azure Service Bus.
    :param queue_name: Nome da fila no Service Bus.
    :param csv_filename: Nome do arquivo CSV a ser enviado.
    """
    try:
        # Conecta ao Service Bus
        servicebus_client = ServiceBusClient.from_connection_string(conn_str=service_bus_connection_string, logging_enable=True)

        with servicebus_client:
            sender = servicebus_client.get_queue_sender(queue_name=queue_name)

            with sender:
                # Lê o conteúdo do arquivo CSV
                with open(csv_filename, "r") as file:
                    csv_content = file.read()

                # Cria a mensagem com o conteúdo do CSV
                message = ServiceBusMessage(csv_content)

                # Envia a mensagem para a fila
                sender.send_messages(message)

                print(f"Arquivo {csv_filename} enviado com sucesso para a fila {queue_name} no Service Bus.")

    except Exception as e:
        print(f"Erro ao enviar o arquivo para o Azure Service Bus: {e}")

import matplotlib.pyplot as plt

def gerar_grafico_dados_sensores(sensor_data, sensor_model, cenario, intervalo_agrupamento=10):
    """
    Gera um gráfico com os dados simulados de um sensor, aplicando suavização e agrupamento.

    :param sensor_data: Lista de dicionários com os dados do sensor.
    :param sensor_model: Nome do modelo do sensor.
    :param cenario: Identificação do cenário.
    :param intervalo_agrupamento: Número de pontos a serem agrupados para suavização.
    """
    # Extrai os dados para o gráfico
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
    plt.savefig(grafico_filename, dpi=300, bbox_inches='tight')
    plt.close()

    print(f"Gráfico gerado: {grafico_filename}")

if __name__ == "__main__":
    benchmark = AlgasBenchmark()
    benchmark.main()