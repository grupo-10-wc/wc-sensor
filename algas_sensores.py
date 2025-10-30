import pdb
import os
import io
import csv
import time
import pandas as pd
import boto3
from typing import Callable
from database import Database
from dotenv import load_dotenv
from simulador_sensores import SimuladorSensor
import datetime

load_dotenv()
BASE_URL = os.getenv("BASE_URL")


class AlgasSimulador:
    def __init__(self):
        os.mkdir('output') if not os.path.exists('output') else None
        os.mkdir('output/plot') if not os.path.exists('output/plot') else None
        os.mkdir('output/csv') if not os.path.exists('output/csv') else None

        self.db = Database(db_name="algas")
        self.db.create_table()

        self.simulador = SimuladorSensor(
            self.db,
            n_dados=500,
            intervalo_ms=1000 * 60 * 30,
            alerta="nenhum")

    def send_to_s3(self, records: list[dict]):
        try:
            df = pd.DataFrame(records)
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(
                    df['created_at']).dt.strftime("%Y-%m-%d %H:%M:%S")

            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION", "us-east-1")
            )

            timestamp = (datetime.date.today()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            object_key = f"{timestamp}/dados.csv"
            bucket_name = os.getenv("AWS_S3_BUCKET_NAME", "raw-wattech10")

            s3_client.upload_fileobj(
                csv_buffer,
                bucket_name,
                object_key
            )

            print(
                f"\033[32mParquet file uploaded to S3: s3://{bucket_name}/{object_key}\033[0m")

        except Exception as e:
            print(f"\033[31mError uploading parquet to S3: {e}\033[0m")

    def simular_dados_sensor(
        self,
        sensor_func: Callable,
        device:str,
        location:str
    ):
        """
        Executa a simulacao de dados para uma funcao de sensor.

        :param sensor_func: Funcao de simulacao de sensor a ser executada.
        :param cenario: Identificacao do cenArio.
        :return: Dados simulados do sensor.
        """
        sensor_csv_filename = f"output/csv/cenario_{
            sensor_func.__name__}_sensores.csv"

        with open(sensor_csv_filename, 'w', newline='') as sensor_csvfile:
            sensor_writer = csv.writer(sensor_csvfile)
            sensor_writer.writerow(['sensor_model',
                                    'measure_unit',
                                    'device',
                                    'location',
                                    'data_type',
                                    'data',
                                    'created_at'])

            print(f"Iniciando simulacao para o sensor {sensor_func.__name__}...")
            dados = sensor_func(device, location)

            for record in dados:
                sensor_writer.writerow(record.values())

            print(f"Simulacao do cenArio {sensor_func.__name__} concluída.")

        return dados

    def enviar_csv_para_s3(self, bucket_name='', prefixo='csv/'):
        s3 = boto3.client('s3')
        folder_path = 'output/csv'

        if not os.path.exists(folder_path):
            print(f"Pasta {folder_path} nao existe.")
            return

        for filename in os.listdir(folder_path):
            pdb.set_trace()
            if filename.endswith(".csv"):
                local_path = os.path.join(folder_path, filename)
                s3_key = f"{prefixo}{filename}"
                try:
                    s3.upload_file(local_path, bucket_name, s3_key)
                    print(f"Arquivo enviado para S3: {s3_key}")
                except Exception as e:
                    print(f"Erro ao enviar {filename} para S3: {e}")

    def run(self):
        """
        Executa a simulacao de dados para todos os cenArios de sensores.
        """
        cenarios = [
        # FLUKE 1735 — medidor trifAsico para cargas grandes
        {"sensor_func": self.simulador.fluke_1735, "device": "Ar-Condicionado 18.000 BTU", "location": "Sala de Reunioes"},
        {"sensor_func": self.simulador.fluke_1735, "device": "Ar-Condicionado 15.000 BTU", "location": "Auditorio"},
        {"sensor_func": self.simulador.fluke_1735, "device": "Ar-Condicionado 12.000 BTU", "location": "Sala de Servidores"},
        {"sensor_func": self.simulador.fluke_1735, "device": "Aquecedor de Ambiente", "location": "Deposito"},

        # SHELLY EM — monitoramento de circuito geral ou setorial
        {"sensor_func": self.simulador.shelly_em, "device": "Disjuntor Geral", "location": "Quadro de Distribuicao"},
        {"sensor_func": self.simulador.shelly_em, "device": "Circuito de Iluminacao", "location": "Corredor Principal"},
        {"sensor_func": self.simulador.shelly_em, "device": "Circuito de Tomadas", "location": "Sala de Engenharia"},
        {"sensor_func": self.simulador.shelly_em, "device": "Painel de Energia", "location": "Subestacao Interna"},

        # SONOFF POW R3 — medicao direta em cargas de tomada
        {"sensor_func": self.simulador.sonoff_pow_r3, "device": "Lâmpada Incandescente - 100 W", "location": "Escritorio"},
        {"sensor_func": self.simulador.sonoff_pow_r3, "device": "Multiprocessador", "location": "Copa"},
        {"sensor_func": self.simulador.sonoff_pow_r3, "device": "Ventilador Pequeno", "location": "Sala de Atendimento"},
        {"sensor_func": self.simulador.sonoff_pow_r3, "device": "Impressora", "location": "Area Administrativa"},

        # PZEM-004T — sensor monofAsico de energia para cargas médias
        {"sensor_func": self.simulador.pzem_004t, "device": "Aquecedor de Ambiente", "location": "Sala Técnica"},
        {"sensor_func": self.simulador.pzem_004t, "device": "Ar-Condicionado 10.000 BTU", "location": "Escritorio"},
        {"sensor_func": self.simulador.pzem_004t, "device": "Circulador de Ar Grande", "location": "Galpao de Producao"},
        {"sensor_func": self.simulador.pzem_004t, "device": "TV em Cores - 20", "location": "Sala de Espera"},

        # HMS M21 — medidor modular de energia, versAtil para cargas diversas
        {"sensor_func": self.simulador.hms_m21, "device": "TV em Cores - 29", "location": "Sala de Descanso"},
        {"sensor_func": self.simulador.hms_m21, "device": "TV em Cores - 14", "location": "Recepcao"},
        {"sensor_func": self.simulador.hms_m21, "device": "Lâmpada Incandescente - 60 W", "location": "Corredor"},
        {"sensor_func": self.simulador.hms_m21, "device": "Multiprocessador", "location": "Copa"},

        # CT CLAMP — transformador de corrente, ideal para monitoramento de ramais e cargas pesadas
        {"sensor_func": self.simulador.ct_clamp, "device": "Ar-Condicionado 12.000 BTU", "location": "Recepcao"},
        {"sensor_func": self.simulador.ct_clamp, "device": "Ar-Condicionado 7.500 BTU", "location": "Sala de Supervisao"},
        {"sensor_func": self.simulador.ct_clamp, "device": "Circulador de Ar Pequeno/Médio", "location": "Oficina"},
        {"sensor_func": self.simulador.ct_clamp, "device": "Ventilador Pequeno", "location": "Area de Producao"}
    ]


        df_dados = pd.DataFrame()

        for cenario in cenarios:
            print(f"Executando simulacao para o sensor {cenario['sensor_func'].__name__}...")
            dados = self.simular_dados_sensor(**cenario)
            df_dados = pd.concat([df_dados, pd.DataFrame(dados)])

        df_dados.to_csv("output/csv/dados.csv", index=False, sep=';')

        try:
            self.send_to_s3(df_dados.to_dict("records"))
            print("\033[32mDados enviados para o S3 com sucesso!\033[0m")
        except ValueError as e:
            print(f"\033[31m{e} sem acesso ao S3 !!!\033[0m")
        except Exception as e:
            print(f"Erro ao enviar dados para o S3: {e}")

        print("Simulacao de todos os cenArios concluída!")


if __name__ == "__main__":
    simulador = AlgasSimulador()
    simulador.run()
    simulador.enviar_csv_para_s3()
