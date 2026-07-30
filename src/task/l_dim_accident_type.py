"""Load 階段：將事故類別維度資料 upsert 進 `dim_accident_type`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_dim_accident_type(
    df_dim_accident_type: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將事故類別維度資料寫入 `dim_accident_type`。

    Parameters:
        df_dim_accident_type (pandas.DataFrame): 待寫入的事故類別維度資料。
        database (str | None): 目標資料庫名稱。
    """
    upsert_to_table(
        df_dim_accident_type,
        table="dim_accident_type",
        update_columns=["accident_category"],
        database=database,
    )
