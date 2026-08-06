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


# 2. 查詢事故事實表且串接日期維度表取得事故對應年月日
# 動態撈取事故資料，並客製化 tooltip
def get_accident_table_with_main_day(
    start_date: tuple[int] | None = None, end_date: tuple[int] | None = None
) -> pd.DataFrame:
    """查詢事故主檔並串接日期維度，附帶預先組好的地圖 tooltip 文字。

    起訖日期都給定且格式正確時才會加上日期範圍條件，否則撈全表。tooltip 文字
    在查詢階段就先組好，可減輕前端地圖渲染迴圈的運算負擔。

    Args:
        start_date (tuple[int] | None): 起始日期，形如 `(2024, 1, 1)` 的三個整數。
        end_date (tuple[int] | None): 結束日期，格式同上。

    Returns:
        pandas.DataFrame: 事故資料，查詢結果非空時多一個 `tooltip_text` 欄，形如：

            accident_id      accident_date  accident_time  is_holiday  death_count  injury_count  longitude   latitude
            2024010100000001  2024-01-01     08:15:00       1           0            1             121.552300  25.088100

            其中 `tooltip_text` 是三行文字（以換行符分隔）：

                事故日期時間：2024-01-01 08:15:00
                死亡：0 人
                受傷：1 人

    Raises:
        SQLAlchemyError: 連線或查詢失敗。
    """
    query_str = """SELECT m.accident_id,
                      d.accident_date,
                      m.accident_time,
                      d.accident_weekday,
                      d.is_holiday,
                      d.national_activity,
                      m.death_count,
                      m.injury_count,
                      m.longitude,
                      m.latitude
                    FROM fact_accident_main m
                        JOIN dim_accident_day d
                            ON m.day_id = d.day_id
                """
    if start_date and end_date:
        if (
            len(start_date) == 3
            and len(end_date) == 3
            and all([isinstance(i, int) for i in start_date])
            and all([isinstance(j, int) for j in end_date])
        ):
            start_date_str = "-".join((str(i) for i in start_date))
            end_date_str = "-".join((str(j) for j in end_date))
            query_str += f"""WHERE accident_date
                                BETWEEN '{start_date_str}' AND '{end_date_str}';
                            """
    else:
        query_str += ";"

    df_acc_dj_cross_time = get_table_from_sqlserver(
        query_str, database="traffic_accidents"
    )

    # 計算邏輯：預先組合好 tooltip 文字
    # 在資料撈取階段就先算好地圖所需的 tooltip 字串，可減輕前端 Folium 渲染迴圈時的運算負擔
    # 只做 tooltip（滑鼠滑過才顯示）
    if not df_acc_dj_cross_time.empty:
        cols = df_acc_dj_cross_time.columns
        if all(
            [
                n in cols
                for n in (
                    "accident_date",
                    "accident_time",
                    "death_count",
                    "injury_count",
                )
            ]
        ):
            df_acc_dj_cross_time["tooltip_text"] = df_acc_dj_cross_time.apply(
                lambda row: (
                    f"事故日期時間：{str(row['accident_date'])}"
                    f" {str(row['accident_time']).split()[-1]}\n"
                    f"死亡：{int(row['death_count'])} 人\n"
                    f"受傷：{int(row['injury_count'])} 人"
                ),
                axis=1,
            )
    return df_acc_dj_cross_time


# 3. 查詢用路環境
def get_accident_table_with_env() -> pd.DataFrame:
    """查詢事故環境事實表全表。

    一件事故一列，記錄事發當下的天氣、照明、速限與路面狀況等環境條件。

    Returns:
        pandas.DataFrame: 事故環境資料，形如：

            accident_id      weather_condition  light_condition  speed_limit_primary_party  road_surface_condition
            2024010100000001  晴                 日間自然光線      50                         乾燥
            2024010100000002  雨                 夜間有照明        60                         濕潤

    Raises:
        SQLAlchemyError: 連線或查詢失敗。
    """
    df_acc_env = get_table_from_sqlserver(
        """SELECT * FROM fact_accident_env;
                                            """,
        database="traffic_accidents",
    )
    return df_acc_env


# 4. 查詢用路人行為
def get_accident_table_with_human() -> pd.DataFrame:
    """查詢事故當事人行為事實表全表。

    一位當事人一列，因此一件事故通常有多列。

    Returns:
        pandas.DataFrame: 當事人資料，形如：

            person_id  accident_id      party_sequence  gender  age  vehicle_type_major  hit_and_run
            1          2024010100000001  1               男      34   機車                0
            2          2024010100000001  2               女      28   自用小客車          0

    Raises:
        SQLAlchemyError: 連線或查詢失敗。
    """
    df_acc_human = get_table_from_sqlserver(
        """SELECT * FROM fact_accident_human;
                                            """,
        database="traffic_accidents",
    )
    return df_acc_human


def get_accident_table_caused_by_pedestrian(
    dql_str: str | None = None, params: dict | None = None
) -> pd.DataFrame:
    """查詢「行人為肇因」的 mart 層分析表，未給查詢字串時撈全表。

    自訂查詢的座標等條件請走 `params` 傳值，不要內插進 SQL 字串。

    Args:
        dql_str (str | None): 自訂的 SELECT 敘述，具名佔位符寫成 `:name`。
        params (dict | None): 對應具名佔位符的參數。

    Returns:
        pandas.DataFrame: 分析表資料，形如：

            accident_id      accident_date  city    longitude   latitude    death_count  injury_count
            2024010100000001  2024-01-01     臺北市  121.552300  25.088100   0            1

    Raises:
        SQLAlchemyError: 連線或查詢失敗（含 SQL 語法錯誤、佔位符缺少綁定值）。
    """
    if dql_str is not None:
        # params 必須往下傳，否則帶有 bind parameter 的查詢會缺少綁定值
        df_pesdestrian_causing_accident = get_table_from_sqlserver(
            dql_str, params, database="traffic_accidents"
        )
    else:
        df_pesdestrian_causing_accident = get_table_from_sqlserver(
            """
                                        SELECT * FROM analysis_pesdestrian_causing_accident;
                                                               """,
            database="traffic_accidents",
        )
    return df_pesdestrian_causing_accident


def get_accident_table_pedestrian_involved_in(
    dql_str: str | None = None, params: dict | None = None
) -> pd.DataFrame:
    """查詢「行人涉入」的 mart 層分析表，未給查詢字串時撈全表。

    與「行人為肇因」的差別在於這張表含所有有行人涉入的事故，不論肇責歸屬。
    自訂查詢的座標等條件請走 `params` 傳值，不要內插進 SQL 字串。

    Args:
        dql_str (str | None): 自訂的 SELECT 敘述，具名佔位符寫成 `:name`。
        params (dict | None): 對應具名佔位符的參數，例如
            `{"min_lat": 25.05, "max_lat": 25.12}`。

    Returns:
        pandas.DataFrame: 分析表資料，形如：

            accident_id      accident_date  city    longitude   latitude    death_count  injury_count
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
