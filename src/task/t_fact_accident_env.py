"""Transform 階段：自事故原始 CSV 清洗出事故環境事實 DataFrame。"""

import numpy as np
import pandas as pd

from src.util.logger_crtx import get_logger
from src.util.mysql_utils import get_table_from_sqlserver
from src.util.read_traffic_accident_file import read_traffic_accident_file
from src.util.table_column_map import fact_accident_env_col_origin_map

logger = get_logger(__name__)


def t_fact_accident_env(csvfile_paths: list[str]) -> pd.DataFrame:
    """從事故 CSV 清洗出事故環境事實資料，一件事故一列。

    逐檔讀入後清洗日期、時間、經緯度與速限，合併所有檔案後回頭查四張表取得
    關聯：`dim_accident_day` 取日編號、`dim_road_design` 與 `dim_lane_design`
    取道路與車道設計編號，最後以日期、時間、經緯度四欄比對 `fact_accident_main`
    取得 `accident_id`。因此執行前那三張維度表與事故主檔都必須已經載入。

    一件事故只會有一筆環境資料，所以去重與排序的依據與主檔相同。

    Args:
        csvfile_paths (list[str]): 事故 CSV 的路徑清單，來自 `e_*` 階段的產出。

    Returns:
        pandas.DataFrame: 事故環境資料，空值已轉成 `None` 以便寫入 MySQL，形如：

            accident_id      weather_condition  light_condition  speed_limit_primary_party  road_design_id  lane_design_id  road_surface_condition
            2024010100000001  晴                日間自然光線      50                         3               7               乾燥
            2024010100000002  雨                夜間有照明        60                         5               2               濕潤

    Raises:
        ValueError: `csvfile_paths` 為空，代表上游沒有產出任何 CSV。
        FileNotFoundError: 清單中的某個路徑不存在。
        SQLAlchemyError: 查詢維度表或事故主檔失敗。

    Notes:
        空清單視為故障參考 ADR-0003，中途查維度表取外鍵是刻意的設計，參考 ADR-0010。
    """
    if not csvfile_paths:
        raise ValueError("csvfile_paths 為空，上游未產出任何 CSV 檔")

    all_df = []
    for file_path in csvfile_paths:
        logger.info(f"正在處理csv檔案: {file_path}")
        # 讀取csv檔案、挑欄、改名、去空白
        df = read_traffic_accident_file(file_path, fact_accident_env_col_origin_map)

        # 清理發生日期
        df["accident_date"] = pd.to_datetime(
            df["accident_date"], errors="coerce", format="%Y%m%d"
        )
        df["accident_date"] = df["accident_date"].astype(str)

        # 清理發生時間
        df["accident_time"] = df["accident_time"].astype(str).str.zfill(6)
        df["accident_time"] = df["accident_time"].apply(
            lambda r: r[0:2] + ":" + r[2:4] + ":" + r[4:]
        )
        # 清理經緯度、速限
        df["longitude"] = df["longitude"].astype("float64")
        df["latitude"] = df["latitude"].astype("float64")
        df["speed_limit_primary_party"] = df["speed_limit_primary_party"].astype(
            "int64"
        )

        all_df.append(df)
        logger.info(f"成功讀取csv檔案: {file_path}。此輪得到列數: {len(df)}")

    # union
    df = pd.concat(all_df)

    # 找day_id關聯
    query = "SELECT day_id, accident_date FROM dim_accident_day;"
    df_dim_accident_day = get_table_from_sqlserver(query, database="traffic_accidents")
    df_dim_accident_day["accident_date"] = df_dim_accident_day["accident_date"].astype(
        str
    )

    df_merged = df.merge(
        df_dim_accident_day,
        how="inner",
        left_on="accident_date",
        right_on="accident_date",
    )

    # 找road_design_id關聯
    query = "SELECT * FROM dim_road_design;"
    df_dim_road_design = get_table_from_sqlserver(query, database="traffic_accidents")
    df_merged = df_merged.merge(
        df_dim_road_design,
        how="inner",
        left_on=["road_type_primary_party", "road_form_major", "road_form_minor"],
        right_on=["road_type_primary_party", "road_form_major", "road_form_minor"],
    )
    # 找lane_design_id關聯
    query = "SELECT * FROM dim_lane_design;"
    df_dim_lane_design = get_table_from_sqlserver(query, database="traffic_accidents")
    df_merged = df_merged.merge(
        df_dim_lane_design,
        how="inner",
        left_on=[
            "lane_divider_direction_major",
            "lane_divider_direction_minor",
            "lane_divider_main_general",
            "lane_divider_fast_slow",
            "lane_edge_marking",
        ],
        right_on=[
            "lane_divider_direction_major",
            "lane_divider_direction_minor",
            "lane_divider_main_general",
            "lane_divider_fast_slow",
            "lane_edge_marking",
        ],
    )

    # 找accident_id關聯
    query = """SELECT accident_id, day_id, accident_time, longitude, latitude
                    FROM fact_accident_main;"""
    df_fact_accident_main = get_table_from_sqlserver(
        query, database="traffic_accidents"
    )
    df_fact_accident_main["accident_time"] = (
        df_fact_accident_main["accident_time"]
        .astype(str)
        .str.replace("0 days", "")
        .str.strip()
    )
    df_fact_accident_main["longitude"] = df_fact_accident_main["longitude"].astype(
        "float64"
    )
    df_fact_accident_main["latitude"] = df_fact_accident_main["latitude"].astype(
        "float64"
    )
    df_merged = df_merged.merge(
        df_fact_accident_main,
        how="left",
        left_on=["day_id", "accident_time", "longitude", "latitude"],
        right_on=["day_id", "accident_time", "longitude", "latitude"],
    )

    # 去重，去重邏輯與main表的邏輯一樣，因為一筆案件只會對應一筆環境資料
    df_fact_accident_env = df_merged.drop_duplicates(
        subset=["day_id", "accident_time", "longitude", "latitude"]
    )
    # print(df_fact_accident_env.head())
    # 排序
    df_fact_accident_env = df_fact_accident_env.sort_values(
        by=["day_id", "accident_time", "longitude", "latitude"]
    ).reset_index(drop=True)

    # 留下想要的欄位
    df_fact_accident_env = df_fact_accident_env.loc[
        :,
        [
            "accident_id",
            "weather_condition",
            "light_condition",
            "speed_limit_primary_party",
            "road_design_id",
            "lane_design_id",
            "road_surface_pavement",
            "road_surface_condition",
            "road_surface_defect",
            "road_obstacle",
            "sight_distance_quality",
            "sight_distance",
            "traffic_signal_type",
            "traffic_signal_action",
        ],
    ]

    # 填補空值，將NaN轉換成None
    df_fact_accident_env = df_fact_accident_env.replace({np.nan: None})

    return df_fact_accident_env
