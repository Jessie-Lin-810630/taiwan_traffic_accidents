"""前端的 SQL 查詢層：各事實表與 mart 層分析表的讀取。

本層只發查詢、回傳 DataFrame，不做業務運算，也不碰 UI。
"""

import pandas as pd

from src.util.mysql_utils import get_table_from_sqlserver

# 解決欄位顯示不完整問題: 確保能清楚看到所有欄位
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)


# 1. 查詢夜市事實表，回傳 Pandas DataFrame 以利後續空間運算
def get_night_markets_table() -> pd.DataFrame:
    """查詢夜市事實表全表。

    一個夜市的每個營業日各佔一列，因此同名夜市會出現多次。

    Returns:
        pandas.DataFrame: 夜市資料，形如：

            nightmarket_id  nightmarket_name  city    latitude   longitude   business_days_weekday  googlemap_rating
            1               士林夜市          臺北市  25.088100  121.524300  星期六                 4.2
            2               士林夜市          臺北市  25.088100  121.524300  星期日                 4.2

    Raises:
        SQLAlchemyError: 連線或查詢失敗。
    """
    df_night_markets = get_table_from_sqlserver(
        """SELECT * FROM fact_night_markets;
                                            """,
        database="traffic_accidents",
    )
    return df_night_markets


def get_accident_table_pedestrian_involved_in(
    dql_str: str | None = None, params: dict | None = None
) -> pd.DataFrame:
    r"""查詢「行人涉入」的 mart 層分析表，未給查詢字串時撈全表。

    與「行人為肇因」的差別在於這張表含所有有行人涉入的事故，不論肇責歸屬。
    自訂查詢的座標等條件請走 `params` 傳值，不要內插進 SQL 字串。

    Args:
        dql_str (str | None): 自訂的 SELECT 敘述，具名佔位符寫成 `:name`。
        params (dict | None): 對應具名佔位符的參數，例如
            `{"min_lat": 25.05, "max_lat": 25.12}`。

    Returns:
        pandas.DataFrame: 分析表資料，形如：

            accident_id      accident_date  city    longitude   latitude    death_count  injury_count \n
            2024010100000002  2024-01-01     新北市  121.465800  25.012400   1            0

    Raises:
        SQLAlchemyError: 連線或查詢失敗（含 SQL 語法錯誤、佔位符缺少綁定值）。
    """
    if dql_str is not None:
        # params 必須往下傳，否則帶有 :min_lat 等 bind parameter 的查詢會缺少綁定值
        df_pesdestrian_involving_accident = get_table_from_sqlserver(
            dql_str, params, database="traffic_accidents"
        )
    else:
        df_pesdestrian_involving_accident = get_table_from_sqlserver(
            """
                                        SELECT * FROM analysis_pesdestrian_involving_accident;
                                                               """,
            database="traffic_accidents",
        )
    return df_pesdestrian_involving_accident
