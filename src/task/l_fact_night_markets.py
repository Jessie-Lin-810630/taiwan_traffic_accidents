"""Load 階段：將夜市事實資料 upsert 進 `fact_night_markets`。"""

from datetime import datetime

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_fact_night_markets(
    df_fact_night_markets: pd.DataFrame, database: str | None = None
) -> None:
    """把夜市事實資料寫入 `fact_night_markets`，唯一鍵重複時改為更新。

    唯一鍵是「緯度 + 經度 + 營業星期」的組合，衝突時更新夜市名稱、營業起訖時間、
    邊界端點座標、地圖網址、評分與更新時間。寫入前會蓋上 `updated_on` 時間戳，
    這是本表獨有的處理，因此留在這支 loader 內而不進共用的寫入函式。

    Args:
        df_fact_night_markets (pandas.DataFrame): 待寫入的資料，欄位名須與資料表一致，
            即 `_t_clean_one_night_market()` 展開後的結果。
        database (str | None): 目標資料庫名稱。

    Raises:
        pymysql.MySQLError: 寫入失敗，事務復原後原樣往外拋。
    """
    df_fact_night_markets["updated_on"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    upsert_to_table(
        df_fact_night_markets,
        table="fact_night_markets",
        update_columns=[
            "updated_on",
            "business_hours_closing",
            "business_hours_opening",
            "northeast_latitude",
            "northeast_longitude",
            "southwest_latitude",
            "southwest_longitude",
            "url_to_googlemap",
            "googlemap_rating",
            "nightmarket_name",
        ],
        database=database,
    )
