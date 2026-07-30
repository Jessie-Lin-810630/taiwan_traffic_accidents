"""Load 階段：將事故日維度資料 upsert 進 `dim_accident_day`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_dim_accident_day(
    df_dim_accident_day: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將事故日維度資料寫入 `dim_accident_day`。

    Parameters:
        df_dim_accident_day (pandas.DataFrame): 待寫入的事故日維度資料。
        database (str | None): 目標資料庫名稱。
    """
    upsert_to_table(
        df_dim_accident_day,
        table="dim_accident_day",
        update_columns=["accident_weekday"],
        database=database,
    )
