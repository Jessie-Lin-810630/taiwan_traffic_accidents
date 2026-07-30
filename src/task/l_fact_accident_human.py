"""Load 階段：將事故當事人事實資料 upsert 進 `fact_accident_human`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_fact_accident_human(
    df_fact_accident_human: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將事故當事人事實資料寫入 `fact_accident_human`。

    Parameters:
        df_fact_accident_human (pandas.DataFrame): 待寫入的事故當事人事實資料。
        database (str | None): 目標資料庫名稱。
    """
    upsert_to_table(
        df_fact_accident_human,
        table="fact_accident_human",
        update_columns=["hit_and_run"],
        database=database,
    )
