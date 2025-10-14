import numpy as np
import datetime
import matplotlib.pyplot as plt
from random import randint
from database import Database


class SimuladorSensor:
    def __init__(self, db: Database, n_dados: int, intervalo_ms: int, alerta: str = "nenhum"):
        self.db = db
        self.sensores = db.sensores()
        self.n_dados = n_dados
        self.intervalo_ms = intervalo_ms
        self.alerta = alerta.lower()

    def batch_insert(self, records, batch_size=1000):
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            self.db.db_execute(self.sensores.insert().values(batch), commit=True)

    def _generate_timestamps(self):
        start_time = (datetime.datetime.now() - datetime.timedelta(days=60)).replace(minute=0, second=0, microsecond=0)
        return [start_time + datetime.timedelta(hours=i) for i in range(self.n_dados)]


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
        timestamps = self._generate_timestamps()
        payload = []

        ufs = [
            "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
            "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
            "RS", "RO", "RR", "SC", "SP", "SE", "TO"
        ]

        media_consumo = 100  
        desvio_padrao = 30

        for i in range(self.n_dados):
            consumo = np.random.normal(media_consumo, desvio_padrao)
            consumo = max(consumo, 5)  # evita valores muito baixos
            consumo = self._apply_alerta(consumo)

            record = {
                'sensorModel': 'Shelly EM',
                'measureUnit': 'kWh',
                'deviceId': f'Disjuntor Geral {randint(1, 9999):04d}',
                'location': 'Quadro de Energia',
                'dataType': 'Consumo de Energia',
                'data': round(consumo, 2),
                'ts': timestamps[i].strftime('%Y-%m-%d %H:%M:%S')
            }
            payload.append(record)

        self.batch_insert(payload)
        return payload


    def sonoff_pow_r3(self):
        timestamps = self._generate_timestamps()
        payload = []

        for i in range(self.n_dados):
            power = np.abs(np.sin(i / 3) * 100 + np.random.normal(0, 10))
            power = self._apply_alerta(power)

            record = {
                'sensorModel': 'Sonoff Pow R3',
                'measureUnit': 'W',
                'deviceId': f'Sonoff_{randint(1, 9999):04d}',
                'location': 'Tomada',
                'dataType': 'Consumo de Energia',
                'data': round(power, 5),
                'ts': timestamps[i].strftime('%Y-%m-%d %H:%M:%S')
            }
            payload.append(record)

        self.batch_insert(payload)
        return payload

    def pzem_004t(self):
        timestamps = self._generate_timestamps()
        payload = []

        i = 0
        while i < self.n_dados:
            # 95% dos casos: valores próximos de 220V (218-222V)
            if np.random.rand() < 0.95:
                voltage = 220 + np.random.normal(0, 1.2)
                voltage = np.clip(voltage, 218, 222)
                record = {
                    'sensorModel': 'PZEM-004T',
                    'measureUnit': 'V',
                    'deviceId': f'PZEM_{randint(1, 9999):04d}',
                    'location': 'Instalação',
                    'dataType': 'Tensão',
                    'data': round(self._apply_alerta(voltage), 5),
                    'ts': timestamps[i].strftime('%Y-%m-%d %H:%M:%S')
                }
                payload.append(record)
                i += 1
            else:
                # 5% dos casos: criar evento gradual de alerta (rampa)
                rampa_tam = np.random.randint(5, 15)  # duração do evento
                if np.random.rand() < 0.5:
                    # Rampa para baixo (até próximo de 198V)
                    start = 220
                    end = 198 + np.random.uniform(0, 2)
                else:
                    # Rampa para cima (até próximo de 242V)
                    start = 220
                    end = 242 - np.random.uniform(0, 2)
                ramp = np.linspace(start, end, rampa_tam//2).tolist() + np.linspace(end, start, rampa_tam - rampa_tam//2).tolist()
                for v in ramp:
                    if i >= self.n_dados:
                        break
                    v_osc = v + np.random.normal(0, 0.7)  # pequena oscilação
                    v_osc = np.clip(v_osc, min(198, v), max(242, v))
                    record = {
                        'sensorModel': 'PZEM-004T',
                        'measureUnit': 'V',
                        'deviceId': f'PZEM_{randint(1, 9999):04d}',
                        'location': 'Instalação',
                        'dataType': 'Tensão',
                        'data': round(self._apply_alerta(v_osc), 5),
                        'ts': int(timestamps[i].timestamp())
                    }
                    payload.append(record)
                    i += 1

        self.batch_insert(payload)
        return payload
    
    def hms_m21(self):
        n_sensores = 5
        payload = []

        start_time = (datetime.datetime.now() - datetime.timedelta(days=60)).replace(minute=0, second=0, microsecond=0)
        timestamps = [start_time + datetime.timedelta(hours=i) for i in range(self.n_dados)]

        for sensor_id in range(1, n_sensores + 1):
            device = f'HMS_{sensor_id:04d}'
            location = f'Quadro de Energia {sensor_id}'

            for i in range(self.n_dados):
                temperature = 25 + np.random.normal(0, 2)
                temperature = self._apply_alerta(temperature)
                record = {
                    'sensorModel': 'hms_m21',
                    'measureUnit': 'C',
                    'deviceId': device,
                    'location': location,
                    'dataType': 'Temperatura',
                    'data': round(temperature, 2),
                    'ts': timestamps[i].strftime('%Y-%m-%d %H:%M:%S')
                }
                payload.append(record)

        self.batch_insert(payload)
        return payload



    def fluke_1735(self):
        def calcular_fator_potencia(tensao, corrente, angulo_fase):
            angulo_fase_rad = np.radians(angulo_fase)
            potencia_ativa = tensao * corrente * np.cos(angulo_fase_rad)
            potencia_aparente = tensao * corrente
            return potencia_ativa / potencia_aparente
        timestamps = self._generate_timestamps()
        payload = []
        ruido_tensao = np.random.normal(0, 1, self.n_dados)
        ruido_corrente = np.random.normal(0, 0.5, self.n_dados)

        window = int(self.n_dados / 10 / 2)
        ruido_tensao = np.convolve(ruido_tensao, np.ones(window)/window, mode='same')
        ruido_corrente = np.convolve(ruido_corrente, np.ones(window)/window, mode='same')

        tensoes = 220 + ruido_tensao * 5
        correntes = 10 + ruido_corrente * 2

        angulo_base = 23.07
        ruido_angulo = np.random.normal(0, 20, self.n_dados)
        ruido_angulo = np.convolve(ruido_angulo, np.ones(window)/window, mode='same')
        angulos_fase = angulo_base + ruido_angulo

        fatores_potencia = [round(float(calcular_fator_potencia(V, I, ang)), 5) 
                        for V, I, ang in zip(tensoes, correntes, angulos_fase)]
        device = f'Ar Condicionado {randint(1, 9999):04d}'
        for i in range(self.n_dados):
            record = {
                'sensorModel': 'Fluke 1735',
                'measureUnit': '%',
                'deviceId': device,
                'location': 'Sala de reuniões',
                'dataType': 'Fator de Potência',
                'data': fatores_potencia[i],
                'ts': timestamps[i].strftime('%Y-%m-%d %H:%M:%S')
            }
            payload.append(record)
        self.batch_insert(payload)
        return payload
    
    def ct_clamp(self):
        alert_threshold: float = 7.0
        alert_threshold_low: float = 3.0
        np.random.seed(2025)

        ruido_corrente = np.random.normal(0, 30, self.n_dados)
        window = int(self.n_dados / 10 / 2)
        ruido_corrente = np.convolve(ruido_corrente, np.ones(window)/window, mode='same')

        correntes = 5 + ruido_corrente
        device = f'CT Clamp {randint(1, 9999):04d}'

        payload = []
        start_time = (datetime.datetime.now() - datetime.timedelta(days=60)).replace(minute=0, second=0, microsecond=0)

        for i in range(len(correntes)):
            ts = start_time + datetime.timedelta(hours=i)
            record = {
                'sensorModel': 'ct_clamp',
                'measureUnit': 'A',
                'deviceId': device,
                'location': 'Quadro de Energia',
                'dataType': 'Corrente Elétrica',
                'data': round(float(correntes[i]), 5),
                'ts': ts.strftime('%Y-%m-%d %H:%M:%S')
            }
            payload.append(record)

        self.batch_insert(payload)
        return payload


        
        # result = self.get_data_by_device(device)
        
        # timestamps = [record[7] for record in result]
        # corrente_values = [record[6] for record in result]
        
        # alert_times = [timestamps[i] for i in range(len(corrente_values)) if corrente_values[i] > alert_threshold]
        # alert_values = [corrente_values[i] for i in range(len(corrente_values)) if corrente_values[i] > alert_threshold]

        # alert_times_low = [timestamps[i] for i in range(len(corrente_values)) if corrente_values[i] < alert_threshold_low]
        # alert_values_low = [corrente_values[i] for i in range(len(corrente_values)) if corrente_values[i] < alert_threshold_low]
        
        
        # plt.figure(figsize=(10, 6))
        # plt.plot(timestamps, corrente_values, label='Corrente Elétrica (A)', color='blue')
        
        # plt.scatter(alert_times, alert_values, color='red', label='Picos (Alertas)', zorder=5)

        # plt.scatter(alert_times_low, alert_values_low, color='orange', label='Corrente Abaixo do Normal', zorder=5)
        
        # plt.title('Corrente Elétrica ao Longo do Tempo')
        # plt.xlabel('Data e Hora')
        # plt.ylabel('Corrente (A)')
        # plt.grid(True)
        # plt.xticks(rotation=45)
        # plt.tight_layout()
        # plt.legend()
        # plt.savefig('corrente_eletrica.png')

        # return {'status': 'success', 'device': device, 'n_samples': len(payload), 'alerts': len(alert_times)}

    def plot_dados(self, dados, titulo, eixo_x, eixo_y, filename):
        timestamps = [datetime.datetime.strptime(record['ts'], '%Y-%m-%d %H:%M:%S') for record in dados]
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
