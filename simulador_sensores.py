import numpy as np
import datetime
from random import randint
from database import Database


class SimuladorSensor:
    def __init__(
            self,
            db: Database,
            n_dados: int,
            intervalo_ms: int,
            alerta: str = "nenhum"):
        self.db = db
        self.sensores = db.sensores()

        self.n_dados = 24
        self.intervalo_ms = intervalo_ms
        self.alerta = alerta.lower()


    def _generate_timestamps(self):
        start_time = datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0)
        return [
            start_time +
            datetime.timedelta(
                hours=i) for i in range(
                self.n_dados)]

    def _apply_alerta(self, valor_base):
        """
        Aplica o comportamento de alerta ao valor base.

        :param valor_base: Valor base gerado.
        :return: Valor ajustado com base no tipo de alerta.
        """
        if self.alerta == "alto":
            return valor_base * np.random.uniform(5, 10)
        elif self.alerta == "baixo":
            return valor_base * np.random.uniform(0.1, 0.5)
        return valor_base

    def shelly_em(self, device: str = 'Disjuntor Geral',
                  location: str = 'Quadro de Energia'):
        timestamps = self._generate_timestamps()
        payload = []

        media_consumo = 100
        desvio_padrao = 30

        for i in range(self.n_dados):
            consumo = np.random.normal(media_consumo, desvio_padrao)
            consumo = max(consumo, 5)
            consumo = self._apply_alerta(consumo)

            record = {
                'sensorModel': 'Shelly EM',
                'measureUnit': 'kWh',
                'device': device,
                'location': location,
                'dataType': 'Consumo de Energia',
                'data': round(consumo, 2),
                'ts': timestamps[i].strftime('%Y-%m-%d %H:%M:%S')
            }
            payload.append(record)

        return payload

    def sonoff_pow_r3(
            self,
            device: str | None = None,
            location: str = 'Tomada'):
        timestamps = self._generate_timestamps()
        payload = []

        for i in range(self.n_dados):
            power = np.abs(np.sin(i / 3) * 100 + np.random.normal(0, 10))
            power = self._apply_alerta(power)

            _device = device if device is not None else f'Sonoff_{
                randint(
                    1,
                    9999):04d}'

            record = {
                'sensorModel': 'Sonoff Pow R3',
                'measureUnit': 'W',
                'device': _device,
                'location': location,
                'dataType': 'Consumo de Energia',
                'data': round(power, 5),
                'ts': timestamps[i].strftime('%Y-%m-%d %H:%M:%S')
            }
            payload.append(record)

        return payload

    def pzem_004t(self, device: str | None = None,
                  location: str = 'Instalação'):
        timestamps = self._generate_timestamps()
        payload = []

        i = 0
        while i < self.n_dados:

            if np.random.rand() < 0.95:
                voltage = 220 + np.random.normal(0, 1.2)
                voltage = np.clip(voltage, 218, 222)
                _device = device if device is not None else f'PZEM_{
                    randint(
                        1,
                        9999):04d}'
                record = {
                    'sensorModel': 'PZEM-004T',
                    'measureUnit': 'V',
                    'device': _device,
                    'location': location,
                    'dataType': 'Tensão',
                    'data': round(self._apply_alerta(voltage), 5),
                    'ts': timestamps[i].strftime('%Y-%m-%d %H:%M:%S')
                }
                payload.append(record)
                i += 1
            else:
                rampa_tam = np.random.randint(5, 15)
                if np.random.rand() < 0.5:
                    start = 220
                    end = 198 + np.random.uniform(0, 2)
                else:
                    start = 220
                    end = 242 - np.random.uniform(0, 2)
                ramp = np.linspace(
                    start,
                    end,
                    rampa_tam // 2
                ).tolist() + np.linspace(
                    end,
                    start,
                    rampa_tam - rampa_tam // 2
                ).tolist()
                for v in ramp:
                    if i >= self.n_dados:
                        break
                    v_osc = v + np.random.normal(0, 0.7)
                    v_osc = np.clip(v_osc, min(198, v), max(242, v))
                    _device = device if device is not None else f'PZEM_{
                        randint(
                            1,
                            9999):04d}'
                    record = {
                        'sensorModel': 'PZEM-004T',
                        'measureUnit': 'V',
                        'device': _device,
                        'location': location,
                        'dataType': 'Tensão',
                        'data': round(self._apply_alerta(v_osc), 5),
                        'ts': timestamps[i].strftime('%Y-%m-%d %H:%M:%S')
                    }
                    payload.append(record)
                    i += 1

        return payload

    def hms_m21(
            self,
            device: str = 'HMS_{sensor_id:04d}',
            location: str = 'Quadro de Energia'):
        payload = []

        timestamps = self._generate_timestamps()

        for i in range(self.n_dados):
            temperature = 25 + np.random.normal(0, 2)
            temperature = self._apply_alerta(temperature)
            record = {
                'sensorModel': 'hms_m21',
                'measureUnit': 'C',
                'device': device,
                'location': location,
                'dataType': 'Temperatura',
                'data': round(temperature, 2),
                'ts': timestamps[i].strftime('%Y-%m-%d %H:%M:%S')
            }
            payload.append(record)

        return payload

    def fluke_1735(self, device: str = 'Fluke_1735',
                   location: str = 'Sala de reuniões'):
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
        ruido_tensao = np.convolve(
            ruido_tensao,
            np.ones(window) / window,
            mode='same')
        ruido_corrente = np.convolve(
            ruido_corrente,
            np.ones(window) / window,
            mode='same')

        tensoes = 220 + ruido_tensao * 5
        correntes = 10 + ruido_corrente * 2

        angulo_base = 23.07
        ruido_angulo = np.random.normal(0, 20, self.n_dados)
        ruido_angulo = np.convolve(
            ruido_angulo,
            np.ones(window) / window,
            mode='same')
        angulos_fase = angulo_base + ruido_angulo

        fatores_potencia = [
            round(
                float(
                    calcular_fator_potencia(
                        V, I_, ang)), 5) for V, I_, ang in zip(
                tensoes, correntes, angulos_fase)]
        for i in range(self.n_dados):
            record = {
                'sensorModel': 'Fluke 1735',
                'measureUnit': '%',
                'device': device,
                'location': location,
                'dataType': 'Fator de Potência',
                'data': fatores_potencia[i],
                'ts': timestamps[i].strftime('%Y-%m-%d %H:%M:%S')
            }
            payload.append(record)
        return payload

    def ct_clamp(self, device: str | None = None,
                 location: str = 'Quadro de Energia'):
        np.random.seed(2025)

        ruido_corrente = np.random.normal(0, 30, self.n_dados)
        window = int(self.n_dados / 10 / 2)
        ruido_corrente = np.convolve(
            ruido_corrente,
            np.ones(window) / window,
            mode='same')

        correntes = 5 + ruido_corrente
        _device = device if device is not None else f'CT Clamp {
            randint(
                1,
                9999):04d}'

        payload = []
        timestamps = self._generate_timestamps()

        for i in range(len(correntes)):
            ts = timestamps[i]
            record = {
                'sensorModel': 'ct_clamp',
                'measureUnit': 'A',
                'device': _device,
                'location': location,
                'dataType': 'Corrente Elétrica',
                'data': round(float(correntes[i]), 5),
                'ts': ts.strftime('%Y-%m-%d %H:%M:%S')
            }
            payload.append(record)

        return payload
