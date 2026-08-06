"""Load 階段：將事故環境事實資料 upsert 進 `fact_accident_env`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_fact_accident_env(
    df_fact_accident_env: pd.DataFrame, database: str | None = None
) -> None:
    """把事故環境事實資料寫入 `fact_accident_env`，主鍵重複時改為更新。

    主鍵是 `accident_id`（同時是參考 `fact_accident_main` 的外鍵），衝突時更新
    `weather_condition`。寫入前事故主檔與道路、車道兩張維度表必須已存在對應的列，
    否則外鍵約束會擋下。

    Args:
        df_fact_accident_env (pandas.DataFrame): 待寫入的資料，欄位名須與資料表一致，
            即 `t_fact_accident_env()` 的產出。
        database (str | None): 目標資料庫名稱。

    Raises:
        pymysql.MySQLError: 寫入失敗（含外鍵約束不成立），事務復原後原樣往外拋。
    """
    upsert_to_table(
        df_fact_accident_env,
        table="fact_accident_env",
        update_columns=["weather_condition"],
        database=database,
    )
