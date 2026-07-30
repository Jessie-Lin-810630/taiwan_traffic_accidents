"""Load 階段：將事故主檔事實資料 upsert 進 `fact_accident_main`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_fact_accident_main(
    df_fact_accident_main: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將事故主檔事實資料寫入 `fact_accident_main`。

    Parameters:
        df_fact_accident_main (pandas.DataFrame): 待寫入的事故主檔事實資料。
        database (str | None): 目標資料庫名稱。
    """
    upsert_to_table(
        df_fact_accident_main,
        table="fact_accident_main",
        update_columns=["accident_time"],
        database=database,
    )
