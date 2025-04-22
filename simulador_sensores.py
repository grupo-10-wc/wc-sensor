import numpy as np
import datetime
import matplotlib.pyplot as plt
from random import randint
from database import Database


class SimuladorSensor:
    def __init__(self, db: Database, n_dados: int, intervalo_ms: int, alerta: str = "nenhum"):
        """
        Inicializa o simulador de sensores.

        :param db: Instância da classe Database.
        :param n_dados: Número de dados a serem gerados.
        :param intervalo_ms: Intervalo em milissegundos entre os dados.
        :param alerta: Tipo de alerta ("alto", "baixo", "nenhum").
        """
        self.db = db
        self.sensores = db.sensores()
        self.n_dados = n_dados
        self.intervalo_ms = intervalo_ms
        self.alerta = alerta.lower()  # Normaliza o valor para minúsculas

    def batch_insert(self, records, batch_size=1000):
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            self.db.db_execute(self.sensores.insert().values(batch), commit=True)

    def _generate_timestamps(self):
        start_time = datetime.datetime.now()
        return [start_time + datetime.timedelta(milliseconds=i * self.intervalo_ms) for i in range(self.n_dados)]

    def _apply_alerta(self, valor_base):
        """
        Aplica o comportamento de alerta ao valor base.

        :param valor_base: Valor base gerado.
        :return: Valor ajustado com base no tipo de alerta.
        """
        if self.alerta == "alto":
            return valor_base * np.random.uniform(5, 10)  # Simula valores muito altos
        elif self.alerta == "baixo":
            return valor_base * np.random.uniform(0.1, 0.5)  # Simula valores muito baixos
        return valor_base  # Sem alerta

    def shelly_em(self):
        np.random.seed(42)
        timestamps = self._generate_timestamps()
        payload = []

        for i in range(self.n_dados):
            consumo = 1 + np.random.normal(0, 0.05)
            consumo = self._apply_alerta(consumo)

            record = {
                'sensor_model': 'Shelly EM',
                'measure_unit': 'kWh',
                'device': f'Disjuntor Geral {randint(1, 9999):04d}',
                'location': 'Quadro de Energia',
                'data_type': 'Consumo de Energia',
                'data': round(consumo, 5),
                'created_at': timestamps[i]
            }
            payload.append(record)

        self.batch_insert(payload)
        return payload

    def sonoff_pow_r3(self):
        np.random.seed(42)
        timestamps = self._generate_timestamps()
        payload = []

        for i in range(self.n_dados):
            power = np.abs(np.sin(i / 3) * 100 + np.random.normal(0, 10))
            power = self._apply_alerta(power)

            record = {
                'sensor_model': 'Sonoff Pow R3',
                'measure_unit': 'W',
                'device': f'Sonoff_{randint(1, 9999):04d}',
                'location': 'Tomada',
                'data_type': 'Consumo de Energia',
                'data': round(power, 5),
                'created_at': timestamps[i]
            }
            payload.append(record)

        self.batch_insert(payload)
        return payload

    def pzem_004t(self):
        np.random.seed(42)
        timestamps = self._generate_timestamps()
        payload = []

        for i in range(self.n_dados):
            voltage = 220 + np.random.normal(0, 1)
            voltage = self._apply_alerta(voltage)

            record = {
                'sensor_model': 'PZEM-004T',
                'measure_unit': 'V',
                'device': f'PZEM_{randint(1, 9999):04d}',
                'location': 'Instalação',
                'data_type': 'Tensão',
                'data': round(voltage, 5),
                'created_at': timestamps[i]
            }
            payload.append(record)

        self.batch_insert(payload)
        return payload

    def fluke_1735(self):
        np.random.seed(42)
        timestamps = self._generate_timestamps()
        payload = []

        for i in range(self.n_dados):
            tensao = 220 + np.random.normal(0, 1)
            corrente = 10 + np.random.normal(0, 0.5)
            angulo_fase = 23.07 + np.random.normal(0, 20)

            tensao = self._apply_alerta(tensao)
            corrente = self._apply_alerta(corrente)

            fator_potencia = round(tensao * corrente * np.cos(np.radians(angulo_fase)) / (tensao * corrente), 5)

            record = {
                'sensor_model': 'Fluke 1735',
                'measure_unit': 'kW',
                'device': f'Ar Condicionado {randint(1, 9999):04d}',
                'location': 'Sala de Reuniões',
                'data_type': 'Fator de Potência',
                'data': fator_potencia,
                'created_at': timestamps[i]
            }
            payload.append(record)

        self.batch_insert(payload)
        return payload

    def plot_dados(self, dados, titulo, eixo_x, eixo_y, filename):
        timestamps = [record['created_at'] for record in dados]
        valores = [record['data'] for record in dados]

        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, valores, label=eixo_y, color='blue')
        plt.title(titulo)
        plt.xlabel(eixo_x)
        plt.ylabel(eixo_y)
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.legend()
        plt.savefig(filename)