import os
import csv
import time
import json
import pandas as pd
from database import Database
import matplotlib.pyplot as plt
from memory_profiler import memory_usage
from simulador_sensores import SimuladorSensor
from azure.iot.device import IoTHubDeviceClient, Message

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

        self.simulador = SimuladorSensor(self.db, n_dados=1000, intervalo_ms=60000, alerta="nenhum")
        

    def open_iot_hub_connection(self):
        CONNECTION_STRING = "CONNECTION_STRING_DO_TRELLO_AQUI"
        client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
        client.connect()

        return client


    def send_iot_hub_message(self, client, message):
        df = pd.DataFrame(message)
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

        csv_filename = f"output/csv/cenario_{cenario}_benchmark.csv"
        sensor_csv_filename = f"output/csv/cenario_{cenario}_sensores.csv"

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
        # service_bus_connection_string = 'COLOCAR AQUI'
        # queue_name = "dados-sensores"

        # Gera o gráfico com os dados dos sensores
        try:
            client = self.open_iot_hub_connection()
            self.send_iot_hub_message(client, dados)
            client.disconnect()
        except ValueError as e:
            print(f"\033[31m{e} COLOCAR A CONNECTION STRING QUE ESTA NO TRELLO AQUI!!!\033[0m")
        except Exception as e:
            print(f"Erro ao enviar dados para o Azure: {e}")
        self.gerar_grafico_dados_sensores(dados, sensor_func.__name__, cenario)

        # Cria um DataFrame com os resultados do benchmark
        df_results = pd.DataFrame({
            'tempo': tempo,
            'blocos': blocos,
            'memoria': memoria,
            'cenario': cenario
        })

        return df_results


    def run(self):
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
        fig_tempo_blocos.savefig('output/plot/benchmark_tempo.png', dpi=300, bbox_inches='tight')
        fig_mem_blocos.savefig('output/plot/benchmark_memoria.png', dpi=300, bbox_inches='tight')

        print("Benchmarks concluídos e gráficos salvos.")


if __name__ == "__main__":
    benchmark = AlgasBenchmark()
    benchmark.run()