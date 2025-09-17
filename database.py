import os
from sqlalchemy import MetaData, Column, Table, create_engine, INTEGER, String, Float, TIMESTAMP, BIGINT
from sqlalchemy.orm import sessionmaker

class Database:
    def __init__(self, db_name='sensores'):
        self.meta = MetaData()
        db_path = os.path.join(os.path.dirname(__file__), f'{db_name}.db')
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.Session = sessionmaker(bind=self.engine)

    def connect(self):
        return self.Session()

    def db_execute(self, query, commit=False):
        with self.connect() as session:
            result = session.execute(query)
            if commit:
                session.commit()
            return result

    def create_table(self):
        self.sensores()
        self.teste_carga()  # Adiciona a criação da tabela teste_carga
        self.meta.create_all(self.engine)

    def sensores(self) -> Table:
        return Table('sensores', self.meta,
                     Column('id', INTEGER, primary_key=True),
                     Column('sensorModel', String),
                     Column('measureUnit', String),
                     Column('deviceId', String),
                     Column('location', String),
                     Column('dataType', String, index=True),
                     Column('data', Float),
                     Column('ts', BIGINT),
                     extend_existing=True
                     )

    def teste_carga(self) -> Table:
        return Table('teste_carga', self.meta,
                     Column('id', INTEGER, primary_key=True),
                     Column('tempo', Float),
                     Column('blocos', Float),
                     Column('memoria', Float),
                     Column('cenario', String),
                     extend_existing=True
                     )