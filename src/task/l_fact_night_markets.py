"""Load 階段：將夜市事實資料 upsert 進 `fact_night_markets`。"""

from datetime import datetime

import pandas as pd

from src.util.mysql_utils import upsert_to_table


def l_fact_night_markets(
    df_fact_night_markets: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將夜市事實資料寫入 `fact_night_markets`。

    寫入前會蓋上 `updated_on` 時間戳。此為本表獨有的處理，
    因此留在本 loader 內，不進共用的 `upsert_to_table()`。

    Parameters:
        df_fact_night_markets (pandas.DataFrame): 待寫入的夜市事實資料。
        database (str | None): 目標資料庫名稱。
    """
    df_fact_night_markets["updated_on"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    upsert_to_table(
        df_fact_night_markets,
        table="fact_night_markets",
        update_columns=[
            "updated_on",
            "business_hours_closing",
            "business_hours_opening",
        ],
        database=database,
    )
