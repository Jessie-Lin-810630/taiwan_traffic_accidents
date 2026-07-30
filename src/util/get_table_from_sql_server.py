"""以 DQL 查詢 MySQL 資料表並回傳 DataFrame 的共用工具。"""

import pandas as pd
from sqlalchemy import text

from src.util.mysql_utils import create_engine_to_mysql


def get_table_from_sqlserver(
    dql_str: str, params: dict | None = None, *, database: str | None = None
) -> pd.DataFrame:
    """Read the desingated table in MySQL server."""
    # 準備與MySQL server的連線
    engine = create_engine_to_mysql(database)

    with engine.connect() as conn:
        result = conn.execute(text(str(dql_str)), parameters=params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df
