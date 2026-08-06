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
    """濾出與事故時間地點相符的天氣觀測，並生成業務唯一鍵。

    整年逐小時的天氣資料是上億列，真正有用的只有「事故發生的那個時間、那個
    觀測點」的那些列，因此這裡以時間與座標做 inner join 壓到百萬列等級。merge
    的兩側經緯度都會再走一次 `round_to_weather_grid()` 當防線 —— 只要有一側用了
    別的算法，就會一列都對不上而且不會報錯。

    唯一鍵不直接用「觀測時間 + 經緯度」，而是把三者組成字串後取 SHA-256 前 32 碼
    存成 `hash_value`，因為經緯度是浮點數，資料庫與 pandas 讀出來的表示可能不同，
    直接當唯一鍵會讓寫入前後的判定不一致。同一筆天氣觀測可能對應多個事故地點，
    因此最後依 hash 去重。

    Args:
        df_weather_raw (pandas.DataFrame): 整年度的原始天氣觀測，需含
            `datetime_ISO8601`、`latitude_round`、`longitude_round` 與各氣象欄位。
        df_all_acc_loc (pandas.DataFrame): 每筆事故的進位座標與整點化時間，
            即 `e_get_all_acc_geo()` 的產出。

    Returns:
        pandas.DataFrame: 可直接寫入 `fact_hourly_weather` 的資料，空值已轉成
            `None`，形如：

            observation_datetime  temperature_degree  rain_within_hour_mm  weather_code  longitude_round  latitude_round  hash_value
            2024-01-01 08:00:00   16.40               0.00                 3             121.55           25.05           3f2a...c81d
            2024-01-01 10:00:00   18.10               0.20                 61            120.65           24.15           9b7e...40aa

            兩側沒有任何時間與地點相符的列時，回傳空 DataFrame。

    Raises:
        KeyError: 任一側缺少 merge 或挑欄所需的欄位。

    Notes:
        兩側座標必須走同一支進位函式，參考 ADR-0012。
    """
    # 1. 清理df_weather_raw統一成YYYY-mm-dd HH:MM:SS的格式並先維持字串
    df_weather_raw["datetime_ISO8601"] = (
        df_weather_raw["datetime_ISO8601"]
        .astype(str)
        .str.replace("T", " ")
        .str.replace(":00", ":00:00")
    )

    # 2. 兩邊的經緯度都走同一支進位函式，下方的 merge 才接得起來
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
