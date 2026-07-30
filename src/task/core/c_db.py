"""純 SQL 查詢層：各事實表與分析表的讀取。"""

import pandas as pd

from src.util.get_table_from_sql_server import get_table_from_sqlserver

# 解決欄位顯示不完整問題: 確保能清楚看到所有欄位
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)


# 1. 查詢夜市事實表，回傳 Pandas DataFrame 以利後續空間運算
def get_night_markets_table() -> pd.DataFrame:
    """查詢夜市事實表。"""
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
    """查詢事故主檔並串接日期維度，附帶預先組好的地圖 tooltip 文字。"""
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
    """查詢事故環境事實表。"""
    df_acc_env = get_table_from_sqlserver(
        """SELECT * FROM fact_accident_env;
                                            """,
        database="traffic_accidents",
    )
    return df_acc_env


# 4. 查詢用路人行為
def get_accident_table_with_human() -> pd.DataFrame:
    """查詢事故當事人行為事實表。"""
    df_acc_human = get_table_from_sqlserver(
        """SELECT * FROM fact_accident_human;
                                            """,
        database="traffic_accidents",
    )
    return df_acc_human


def get_accident_table_caused_by_pedestrian(
    dql_str: str | None = None, params: dict | None = None
) -> pd.DataFrame:
    """查詢「行人為肇因」的分析表；未給 dql_str 時撈全表。"""
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
    """查詢「行人涉入」的分析表；未給 dql_str 時撈全表。"""
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
