"""Load 階段：將事故環境事實資料 upsert 進 `fact_accident_env`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_fact_accident_env(
    df_fact_accident_env: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將事故環境事實資料寫入 `fact_accident_env`。

    Parameters:
        df_fact_accident_env (pandas.DataFrame): 待寫入的事故環境事實資料。
        database (str | None): 目標資料庫名稱。
    """
    upsert_to_table(
        df_fact_accident_env,
        table="fact_accident_env",
        update_columns=["weather_condition"],
        database=database,
    )
