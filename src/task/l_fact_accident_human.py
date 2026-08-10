"""Load 階段：將事故當事人事實資料 upsert 進 `fact_accident_human`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_fact_accident_human(
    df_fact_accident_human: pd.DataFrame, database: str | None = None
) -> None:
    """把事故當事人事實資料寫入 `fact_accident_human`，唯一鍵重複時改為更新。

    唯一鍵是 `row_hash`，衝突時更新 `hit_and_run`。寫入前 `fact_accident_main`
    必須已存在對應的 `accident_id`，否則外鍵約束會擋下。

    Args:
        df_fact_accident_human (pandas.DataFrame): 待寫入的資料，欄位名須與資料表一致，
            即 `t_fact_accident_human()` 的產出。
        database (str | None): 目標資料庫名稱。

    Raises:
        pymysql.MySQLError: 寫入失敗（含外鍵約束不成立），事務復原後原樣往外拋。
    """
    upsert_to_table(
        df_fact_accident_human,
        table="fact_accident_human",
        update_columns=["hit_and_run"],
        database=database,
    )
