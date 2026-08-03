"""Transform 階段：自事故原始 CSV 清洗出事故主檔事實 DataFrame。"""

import numpy as np
import pandas as pd

from src.util.logger_crtx import get_logger
from src.util.mysql_utils import get_table_from_sqlserver
from src.util.read_traffic_accident_file import read_traffic_accident_file
from src.util.table_column_map import fact_accident_main_col_origin_map

logger = get_logger(__name__)


def t_fact_accident_main(csvfile_paths: list[str]) -> pd.DataFrame:
    """S"""
    if not csvfile_paths:
        raise ValueError("csvfile_paths 為空，上游未產出任何 CSV 檔")

    all_df = []
    for file_path in csvfile_paths:
        logger.info(f"正在處理csv檔案: {file_path}")
        # 讀取csv檔案、挑欄、改名、去空白
        df = read_traffic_accident_file(file_path, fact_accident_main_col_origin_map)

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
        # 清理死傷人數
        df["death_count"] = df["casualties_count"].apply(
            lambda r: int(r.split(";")[0].replace("死亡", ""))
        )
        df["injury_count"] = df["casualties_count"].apply(
            lambda r: int(r.split(";")[1].replace("受傷", ""))
        )

        # 清理經緯度
        df["longitude"] = df["longitude"].astype("float64")
        df["latitude"] = df["latitude"].astype("float64")

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

    # 找accident_type_id關聯
    query = "SELECT * FROM dim_accident_type;"
    df_dim_accident_type = get_table_from_sqlserver(query, database="traffic_accidents")
    df_merged = df_merged.merge(
        df_dim_accident_type,
        how="inner",
        left_on=[
            "accident_category",
            "accident_position_major",
            "accident_position_minor",
            "accident_type_major",
            "accident_type_minor",
        ],
        right_on=[
            "accident_category",
            "accident_position_major",
            "accident_position_minor",
            "accident_type_major",
            "accident_type_minor",
        ],
    )

    # 去重
    df_fact_accident_main = df_merged.drop_duplicates(
        subset=["day_id", "accident_time", "longitude", "latitude"]
    )
    # 排序
    df_fact_accident_main = df_fact_accident_main.sort_values(
        by=["day_id", "accident_time", "longitude", "latitude"]
    ).reset_index(drop=True)

    # 生成PK (YYYYMMDD + 8位流水號)
    df_fact_accident_main["prefix"] = (
        df_fact_accident_main["accident_date"].astype(str).str.replace("-", "")
    )
    df_fact_accident_main["cumcount"] = (
        df_fact_accident_main.groupby("accident_date").cumcount() + 1
    )
    df_fact_accident_main["accident_id"] = df_fact_accident_main[
        "prefix"
    ] + df_fact_accident_main["cumcount"].astype(str).str.zfill(8)

    # 留下想要的欄位
    df_fact_accident_main = df_fact_accident_main.loc[
        :,
        [
            "accident_id",
            "accident_type_id",
            "day_id",
            "accident_time",
            "death_count",
            "injury_count",
            "longitude",
            "latitude",
        ],
    ]

    # 填補空值，將NaN轉換成None
    df_fact_accident_main = df_fact_accident_main.replace({np.nan: None})

    return df_fact_accident_main
