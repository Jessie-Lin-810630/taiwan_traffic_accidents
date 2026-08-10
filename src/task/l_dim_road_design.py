"""Load 階段：將道路設計維度資料 upsert 進 `dim_road_design`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_dim_road_design(
    df_dim_road_design: pd.DataFrame, database: str | None = None
) -> None:
    """把道路設計維度資料寫入 `dim_road_design`，唯一鍵重複時改為更新。

    唯一鍵是道路類別與道路型態大小類三欄的組合，衝突時更新 `road_form_minor`，
    因此同一份資料重跑不會產生重複列。

    Args:
        df_dim_road_design (pandas.DataFrame): 待寫入的資料，欄位名須與資料表一致，
            即 `t_dim_road_design()` 的產出。
        database (str | None): 目標資料庫名稱。

    Raises:
        pymysql.MySQLError: 寫入失敗，事務復原後原樣往外拋。
    """
    upsert_to_table(
        df_dim_road_design,
        table="dim_road_design",
        update_columns=["road_form_minor"],
        database=database,
    )
