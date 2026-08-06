"""Load 階段：將事故類別維度資料 upsert 進 `dim_accident_type`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_dim_accident_type(
    df_dim_accident_type: pd.DataFrame, database: str | None = None
) -> None:
    """把事故類別維度資料寫入 `dim_accident_type`，唯一鍵重複時改為更新。

    唯一鍵是事故級別、位置大小類與型態大小類五欄的組合，衝突時更新
    `accident_category`，因此同一份資料重跑不會產生重複列。

    Args:
        df_dim_accident_type (pandas.DataFrame): 待寫入的資料，欄位名須與資料表一致，
            即 `t_dim_accident_type()` 的產出。
        database (str | None): 目標資料庫名稱。

    Raises:
        pymysql.MySQLError: 寫入失敗，事務復原後原樣往外拋。
    """
    upsert_to_table(
        df_dim_accident_type,
        table="dim_accident_type",
        update_columns=["accident_category"],
        database=database,
    )
