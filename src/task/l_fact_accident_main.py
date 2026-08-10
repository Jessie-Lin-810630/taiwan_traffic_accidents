"""Load 階段：將事故主檔事實資料 upsert 進 `fact_accident_main`。"""

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_fact_accident_main(
    df_fact_accident_main: pd.DataFrame, database: str | None = None
) -> None:
    """把事故主檔事實資料寫入 `fact_accident_main`，主鍵重複時改為更新。

    主鍵 `accident_id` 由「日編號 + 時間 + 經緯度」這組唯一鍵雜湊而來，兩者同源，
    因此衝突只代表同一件事故重複出現，要更新的是非鍵欄位 —— 事故類別與死傷人數，
    也就是本函式寫入的欄位裡扣掉主鍵、唯一鍵四欄與 `weather_record_id` 之後剩下的
    全部。寫入前 `dim_accident_day` 與 `dim_accident_type` 必須已存在對應的列，
    否則外鍵約束會擋下。

    唯一鍵那四欄不列入 `update_columns`，因為它們是事故身分的組成，更新它們會讓
    一列的時間與座標來自不同事故；`weather_record_id` 同樣不列入，它由天氣 ETL
    回填、不由事故 ETL 產生，不在本函式的職責範圍內。

    Args:
        df_fact_accident_main (pandas.DataFrame): 待寫入的資料，欄位名須與資料表一致，
            即 `t_fact_accident_main()` 的產出。
        database (str | None): 目標資料庫名稱。

    Raises:
        pymysql.MySQLError: 寫入失敗（含外鍵約束不成立），事務復原後原樣往外拋。

    Notes:
        `update_columns` 的範疇參考 ADR-0014。
    """
    upsert_to_table(
        df_fact_accident_main,
        table="fact_accident_main",
        update_columns=["accident_type_id", "death_count", "injury_count"],
        database=database,
    )
