"""Load 階段：將車道設計維度資料 upsert 進 `dim_lane_design`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_dim_lane_design(
    df_dim_lane_design: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將車道設計維度資料寫入 `dim_lane_design`。

    Parameters:
        df_dim_lane_design (pandas.DataFrame): 待寫入的車道設計維度資料。
        database (str | None): 目標資料庫名稱。
    """
    upsert_to_table(
        df_dim_lane_design,
        table="dim_lane_design",
        update_columns=["lane_edge_marking"],
        database=database,
    )
