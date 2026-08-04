import sys

import pandas as pd
from sqlalchemy import text

# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到 ./utils下的模組或套件
if "/opt/airflow" not in sys.path:
    sys.path.append("/opt/airflow")

# 2. 在sys.path之後才進行import
from src.util.create_engine_conn_tomysql import get_engine_sqlalchemy


def get_table_from_sqlserver(
    dql_str: str, *, database: str | None = None
) -> pd.DataFrame:
    """Read the desingated table in MySQL server."""
    # 準備與MySQL server的連線
    if database:
        engine = get_engine_sqlalchemy(database)
    else:
        engine = get_engine_sqlalchemy()

    with engine.connect() as conn:
        result = conn.execute(text(str(dql_str)))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df
