from contextlib import contextmanager

import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine

@contextmanager
def connect_mysql(config):
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    )

    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception as e:
        raise e
    
class MySQLIOManager(IOManager):
    def __init__(self, config):
        self.config = config

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        pass

    def load_input(self, context: InputContext) -> pd.DataFrame:
        # with connect_mysql(self.config) as conn:
        #     pd_data = pd.read_sql_query(context.upstream_output.name, conn)
        #     return pd_data
        pass

    def extract_data(self, sql: str) -> pd.DataFrame:
        with connect_mysql(self.config) as conn:
            pd_data = pd.read_sql_query(sql, conn)
            return pd_data