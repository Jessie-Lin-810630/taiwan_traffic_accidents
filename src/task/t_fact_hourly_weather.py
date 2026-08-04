"""Transform 階段：把整批天氣觀測濾成與事故時地相符的資料列，並生成業務唯一鍵。"""

import hashlib

import numpy as np
import pandas as pd

from src.task.e_crawling_weather import round_to_weather_grid
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)

# 寫入 fact_hourly_weather 所需的欄位名稱，順序與 merge 後挑欄的順序一致。
_TARGET_COLUMNS = [
    "observation_datetime",
    "temperature_degree",
    "apparent_temperature_degree",
    "rain_within_hour_mm",
    "precipitation_mm",
    "wind_speed_10m_km_per_h",
    "wind_gusts_10m_km_per_h",
    "weather_code",
    "longitude_round",
    "latitude_round",
]


def t_fact_hourly_weather(
    df_weather_raw: pd.DataFrame, df_all_acc_loc: pd.DataFrame
) -> pd.DataFrame:
    """Transform: 濾掉與事故時地無關的天氣觀測，並生成 hash 作為業務唯一鍵。

    整年逐小時的天氣資料量級是上億列，但真正有用的只有「事故發生的那個時間、
    那個觀測點」的那些列，因此這裡以 inner join 把它壓到百萬列等級。

    merge 的兩側都必須經過 `round_to_weather_grid()` ——
    事故側由 `e_get_all_acc_geo()` 完成，天氣側在此正規化一次當防線。
    只要有一側用了別的算法，join 會一列都對不上，而且不會報錯。

    :param df_weather_raw: 從OpenMeteo API下載下來的原始整年度天氣觀測資料，為dataframe
    :type df_weather_raw: pd.DataFrame
    :param df_all_acc_loc: 描述每個車禍地點經緯度進位至氣象網格的結果之dataframe
    :type df_all_acc_loc: pd.DataFrame
    :return: 車禍事故日期時間相近的天氣觀測資料之dataframe，若沒有時間相近的天氣資料，
             則回傳empty dataframe
    :rtype: DataFrame
    """
    # 1. 清理df_weather_raw統一成YYYY-mm-dd HH:MM:SS的格式並先維持字串
    df_weather_raw["datetime_ISO8601"] = (
        df_weather_raw["datetime_ISO8601"]
        .astype(str)
        .str.replace("T", " ")
        .str.replace(":00", ":00:00")
    )

    # 2. 兩邊的經緯度都走同一支進位函式，下方的 merge 才接得起來（ADR-0012）
    df_all_acc_loc["lat_round"] = round_to_weather_grid(df_all_acc_loc["lat_round"])
    df_all_acc_loc["lon_round"] = round_to_weather_grid(df_all_acc_loc["lon_round"])
    df_weather_raw["latitude_round"] = round_to_weather_grid(
        df_weather_raw["latitude_round"]
    )
    df_weather_raw["longitude_round"] = round_to_weather_grid(
        df_weather_raw["longitude_round"]
    )

    # 3. 濾掉跟車禍日與車禍地點不相干的資料列
    df_weather_mrg = df_all_acc_loc.merge(
        df_weather_raw,
        how="inner",
        left_on=["approx_accident_datetime", "lon_round", "lat_round"],
        right_on=["datetime_ISO8601", "longitude_round", "latitude_round"],
        suffixes=["_a", "_w"],
    )

    df_weather_mrg = df_weather_mrg.loc[
        :,
        [
            "datetime_ISO8601",
            "temperature_2m_degree",
            "apparent_temperature_degree",
            "rain_mm",
            "precipitation_mm",
            "wind_speed_10m_km_per_h",
            "wind_gusts_10m_km_per_h",
            "weather_code",
            "longitude_round",
            "latitude_round",
        ],
    ]

    # 4. 正式置換成寫入資料表所需的欄位名稱
    df_weather_mrg.columns = _TARGET_COLUMNS

    # 5. 生成UK。構成業務唯一的是 observation_datetime、longitude_round、latitude_round，
    # 後二者是浮點數；浮點數當 SQL 的 UK 時，資料庫與 pandas 的讀取結果可能不同，
    # 導致業務邏輯在寫入前後不一致。因此在 T 層先生好 hash 當 UK。
    uk = (
        df_weather_mrg["observation_datetime"].astype(str)
        + "|"
        + df_weather_mrg["longitude_round"].astype(str)
        + "|"
        + df_weather_mrg["latitude_round"].astype(str)
    )
    df_weather_mrg["hash_value"] = uk.apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()[:32]
    )

    dup_cnt = df_weather_mrg["hash_value"].duplicated().sum()
    logger.info(
        f"There are {dup_cnt} rows having duplicated combination of "
        f"observation_datetime, long_round and lat_round."
    )

    # 6. 一筆天氣觀測可能對應到多個事故地點，因此去重
    df_weather_mrg = df_weather_mrg.drop_duplicates(subset=["hash_value"])

    # 7. 填補空值
    df_weather_final = df_weather_mrg.where(pd.notnull(df_weather_mrg), None)
    df_weather_final = df_weather_final.replace({np.nan: None})

    logger.info(
        f"Completed data cleaning, got {len(df_weather_final)} rows "
        f"to be loaded to MySQL."
    )
    return df_weather_final
