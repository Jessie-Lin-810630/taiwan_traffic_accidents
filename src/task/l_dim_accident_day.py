"""Load 階段：將事故日維度資料 upsert 進 `dim_accident_day`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_dim_accident_day(
    df_dim_accident_day: pd.DataFrame, database: str | None = None
) -> None:
    """把事故日維度資料寫入 `dim_accident_day`，唯一鍵重複時改為更新。

    唯一鍵是 `accident_date`，衝突時更新 `accident_weekday`，因此同一份資料
    重跑不會產生重複列。

    Args:
        df_dim_accident_day (pandas.DataFrame): 待寫入的資料，欄位名須與資料表一致，
            即 `t_data_for_dim_accident_day()` 的產出。
        database (str | None): 目標資料庫名稱。

    Raises:
        pymysql.MySQLError: 寫入失敗，事務復原後原樣往外拋。
    """
    upsert_to_table(
        df_dim_accident_day,
        table="dim_accident_day",
        update_columns=["accident_weekday"],
        database=database,
    )
