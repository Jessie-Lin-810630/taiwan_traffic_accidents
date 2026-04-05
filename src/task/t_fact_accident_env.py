import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import text
from datetime import datetime, timedelta, timezone
from src.util.table_column_map import fact_accident_env_col_origin_map
from src.util.get_table_from_sql_server import get_table_from_sqlserver
from airflow.models import Variable
from airflow.exceptions import AirflowException


def t_fact_accident_env(csvfile_paths: list[str]) -> pd.DataFrame:
    """s"""
    all_df = []
    for file_path in csvfile_paths:
        print(f"正在處理csv檔案: {file_path}")
        # 讀取csv檔案到DataFrame
        df = pd.read_csv(file_path, encoding="utf-8", skipfooter=2, engine="python")

        # 擷取需要的欄位
        required_columns = [k for k in fact_accident_env_col_origin_map.keys()]
        df = df.loc[:, required_columns]

        # 重新命名欄位
        renamed_required_columns = [fact_accident_env_col_origin_map[k] for k in required_columns]
        df.columns = renamed_required_columns

        # 清理發生日期
        df["accident_date"] = pd.to_datetime(df["accident_date"], errors="coerce",
                                             format="%Y%m%d")
        df["accident_date"] = df["accident_date"].astype(str)

        # 清理發生時間
        df["accident_time"] = df['accident_time'].astype(str).str.zfill(6)
        df["accident_time"] = df["accident_time"].apply(lambda r: r[0:2] + ":" +
                                                        r[2:4] + ":" + r[4:])
        # 清理經緯度、速限
        df["longitude"] = df["longitude"].astype("float64")
        df["latitude"] = df["latitude"].astype("float64")
        df["speed_limit_primary_party"] = df["speed_limit_primary_party"].astype("int64")

        # 去空白
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        all_df.append(df)
        print(f"成功讀取csv檔案: {file_path}。此輪得到列數: {len(df)}")

    # union
    df = pd.concat(all_df)

    # 找day_id關聯
    query = "SELECT day_id, accident_date FROM dim_accident_day;"
    df_dim_accident_day = get_table_from_sqlserver(query, database="traffic_accidents")
    df_dim_accident_day["accident_date"] = df_dim_accident_day["accident_date"].astype(str)

    df_merged = df.merge(df_dim_accident_day, how="inner", left_on="accident_date",
                         right_on="accident_date")

    # 找road_design_id關聯
    query = "SELECT * FROM dim_road_design;"
    df_dim_road_design = get_table_from_sqlserver(query, database="traffic_accidents")
    df_merged = df_merged.merge(df_dim_road_design, how="inner",
                                left_on=["road_type_primary_party",
                                         "road_form_major",
                                         "road_form_minor"],
                                right_on=["road_type_primary_party",
                                          "road_form_major",
                                          "road_form_minor"])
    # 找lane_design_id關聯
    query = "SELECT * FROM dim_lane_design;"
    df_dim_lane_design = get_table_from_sqlserver(query, database="traffic_accidents")
    df_merged = df_merged.merge(df_dim_lane_design, how="inner",
                                left_on=["lane_divider_direction_major",
                                         "lane_divider_direction_minor",
                                         "lane_divider_main_general",
                                         "lane_divider_fast_slow",
                                         "lane_edge_marking"],
                                right_on=["lane_divider_direction_major",
                                          "lane_divider_direction_minor",
                                          "lane_divider_main_general",
                                          "lane_divider_fast_slow",
                                          "lane_edge_marking"])

    # 找accident_id關聯
    query = """SELECT accident_id, day_id, accident_time, longitude, latitude
                    FROM fact_accident_main;"""
    df_fact_accident_main = get_table_from_sqlserver(query, database="traffic_accidents")
    df_fact_accident_main["accident_time"] = df_fact_accident_main["accident_time"].astype(
        str).str.replace("0 days", "").str.strip()
    df_fact_accident_main["longitude"] = df_fact_accident_main["longitude"].astype("float64")
    df_fact_accident_main["latitude"] = df_fact_accident_main["latitude"].astype("float64")
    df_merged = df_merged.merge(df_fact_accident_main, how="left",
                                left_on=["day_id", "accident_time",
                                         "longitude", "latitude"],
                                right_on=["day_id", "accident_time",
                                          "longitude", "latitude"])

    # 去重，去重邏輯與main表的邏輯一樣，因為一筆案件只會對應一筆環境資料
    df_fact_accident_env = df_merged.drop_duplicates(subset=["day_id",
                                                             "accident_time",
                                                             "longitude",
                                                             "latitude"])
    # print(df_fact_accident_env.head())
    # 排序
    df_fact_accident_env = df_fact_accident_env.sort_values(by=["day_id",
                                                                "accident_time",
                                                                "longitude",
                                                                "latitude"]).reset_index(drop=True)

    # 留下想要的欄位
    df_fact_accident_env = df_fact_accident_env.loc[:, ["accident_id", "weather_condition",
                                                        "light_condition", "speed_limit_primary_party",
                                                        "road_design_id", "lane_design_id",
                                                        "road_surface_pavement", "road_surface_condition",
                                                        "road_surface_defect", "road_obstacle",
                                                        "sight_distance_quality", "sight_distance",
                                                        "traffic_signal_type", "traffic_signal_action"
                                                        ]]

    # 填補空值，將NaN轉換成None
    df_fact_accident_env = df_fact_accident_env.replace({np.nan: None})

    return df_fact_accident_env
