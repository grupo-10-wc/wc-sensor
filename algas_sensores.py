import os
import io
import csv
import time
import pandas as pd
import boto3
from typing import Callable 
from database import Database
from dotenv import load_dotenv
# from memory_profiler import memory_usage  #COMENTADO(F44 44F, 4F4) - import não utilizado
from simulador_sensores import SimuladorSensor


load_dotenv()
BASE_URL = os.getenv("BASE_URL")


class AlgasSimulador:
    def __init__(self):
        # Inicializa as pastas de output
        os.mkdir('output') if not os.path.exists('output') else None
        os.mkdir('output/plot') if not os.path.exists('output/plot') else None
        os.mkdir('output/csv') if not os.path.exists('output/csv') else None
        
        # Inicializa o banco de dados (para compatibilidade com o simulador)
        self.db = Database(db_name="algas")
        self.db.create_table()
        
        # Inicializa o simulador de sensores
        self.simulador = SimuladorSensor(self.db, n_dados=500, intervalo_ms=1000*60*30, alerta="nenhum")
        


    def send_to_s3(self, records: list[dict]):
        try:
            # Convert records to DataFrame
            df = pd.DataFrame(records)
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime("%Y-%m-%d %H:%M:%S")
            
            # Create parquet file in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            # Initialize S3 client
            s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION", "us-east-1")
            )
            
            timestamp = time.strftime("%Y%m%d%H%M%S")
            object_key = f"dados/dados_{timestamp}.parquet"
            bucket_name = os.getenv("AWS_S3_BUCKET_NAME", "algas-sensor-data")
            
            s3_client.upload_fileobj(
                parquet_buffer,
                bucket_name,
                object_key
            )
            
            print(f"\033[32mParquet file uploaded to S3: s3://{bucket_name}/{object_key}\033[0m")
            
        except Exception as e:
            print(f"\033[31mError uploading parquet to S3: {e}\033[0m")



    def simular_dados_sensor(self, sensor_func: Callable, cenario: int):
        """
        Executa a simulação de dados para uma função de sensor.
 
        :param sensor_func: Função de simulação de sensor a ser executada.
        :param cenario: Identificação do cenário.
        :return: Dados simulados do sensor.
        """
        sensor_csv_filename = f"output/csv/cenario_{sensor_func.__name__}_sensores.csv"

        with open(sensor_csv_filename, 'w', newline='') as sensor_csvfile:
            sensor_writer = csv.writer(sensor_csvfile)
            sensor_writer.writerow(['sensor_model', 'measure_unit', 'device', 'location', 'data_type', 'data', 'created_at'])

            print(f"Iniciando simulação para o cenário {cenario}...")

            # Executa a simulação do sensor
            dados = sensor_func()

            # Salva os dados dos sensores no CSV
            for record in dados:
                sensor_writer.writerow(record.values())

            print(f"Simulação do cenário {cenario} concluída.")

        return dados

    def enviar_csv_para_s3(self, bucket_name='', prefixo='csv/'):        
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
        """
        Executa a simulação de dados para todos os cenários de sensores.
        """
        cenarios = [
            {"cenario": 4, "sensor_func": self.simulador.fluke_1735},
            {"cenario": 1, "sensor_func": self.simulador.shelly_em},
            {"cenario": 2, "sensor_func": self.simulador.sonoff_pow_r3},
            {"cenario": 3, "sensor_func": self.simulador.pzem_004t},
            {"cenario": 5, "sensor_func": self.simulador.hms_m21},
            {"cenario": 6, "sensor_func": self.simulador.ct_clamp},
        ]

        df_dados = pd.DataFrame()

        for cenario in cenarios:
            print(f"Executando simulação para o cenário {cenario['cenario']}...")
            dados = self.simular_dados_sensor(cenario["sensor_func"], cenario["cenario"])
            df_dados = pd.concat([df_dados, pd.DataFrame(dados)])

        # Salva todos os dados em um arquivo CSV consolidado
        df_dados.to_csv("output/csv/dados.csv", index=False)
        
        try:
            # Envia dados para S3
            self.send_to_s3(df_dados.to_dict("records"))
            print("\033[32mDados enviados para o S3 com sucesso!\033[0m")
        except ValueError as e:
            print(f"\033[31m{e} sem acesso ao S3 !!!\033[0m")
        except Exception as e:
            print(f"Erro ao enviar dados para o S3: {e}")
            
        print("Simulação de todos os cenários concluída!")
            



if __name__ == "__main__":
    simulador = AlgasSimulador()
    simulador.run()
    simulador.enviar_csv_para_s3()