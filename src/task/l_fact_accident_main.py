"""Load 階段：將事故主檔事實資料 upsert 進 `fact_accident_main`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_fact_accident_main(
    df_fact_accident_main: pd.DataFrame, database: str | None = None
) -> None:
    """把事故主檔事實資料寫入 `fact_accident_main`，主鍵重複時改為更新。

    主鍵是 `accident_id`，另有「日編號 + 時間 + 經緯度」的唯一鍵，衝突時更新
    `accident_time`。寫入前 `dim_accident_day` 與 `dim_accident_type` 必須已存在
    對應的列，否則外鍵約束會擋下。

    Args:
        df_fact_accident_main (pandas.DataFrame): 待寫入的資料，欄位名須與資料表一致，
            即 `t_fact_accident_main()` 的產出。
        database (str | None): 目標資料庫名稱。

    Raises:
        pymysql.MySQLError: 寫入失敗（含外鍵約束不成立），事務復原後原樣往外拋。
    """
    upsert_to_table(
        df_fact_accident_main,
        table="fact_accident_main",
        update_columns=["accident_time"],
        database=database,
    )
