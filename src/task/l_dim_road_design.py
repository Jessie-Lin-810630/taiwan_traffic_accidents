"""Load 階段：將道路設計維度資料 upsert 進 `dim_road_design`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_dim_road_design(
    df_dim_road_design: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將道路設計維度資料寫入 `dim_road_design`。

    Parameters:
        df_dim_road_design (pandas.DataFrame): 待寫入的道路設計維度資料。
        database (str | None): 目標資料庫名稱。
    """
    upsert_to_table(
        df_dim_road_design,
        table="dim_road_design",
        update_columns=["road_form_minor"],
        database=database,
    )
