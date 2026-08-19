"""Transform 階段：把整批天氣觀測濾成與事故時地相符的資料列，並生成業務唯一鍵。"""

import hashlib

import numpy as np
import pandas as pd

from src.task.e_crawling_weather import round_to_weather_grid
from src.util.logger_crtx import get_logger
from src.util.mysql_utils import get_table_from_sqlserver

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


def t_fact_main_ref_to_fact_weather(
    target_year: int,
    df_all_acc_loc: pd.DataFrame,
    *,
    database: str | None = None,
) -> pd.DataFrame:
    """回讀已寫入 MySQL 的天氣觀測資料，算出每筆事故該指向哪一筆 `weather_record_id`。

    `fact_accident_main.weather_record_id` 目前採 soft reference，值參考到
    `fact_hourly_weather` 的 auto increment primary key，
    因此必須等天氣資料寫進 MySQL 之後回讀才能取得。

    Args:
        target_year (int): 要回填的年份，對應 `observation_datetime` 的西元年。
        df_all_acc_loc (pandas.DataFrame): 每筆事故的進位座標與整點化時間，
            即 `e_get_all_acc_geo()` 的產出。
        database (str | None): 資料表所在的資料庫名稱。

    Returns:
        pandas.DataFrame: 兩欄的對照表，形如：

            accident_id  weather_record_id
            1130101001   1024
            1130101002   2371

        沒有任何事故配對到天氣觀測時，回傳空 DataFrame（欄位仍在）。

    Raises:
        SQLAlchemyError: 查詢失敗。
        KeyError: 任一側缺少 merge 所需的欄位。

    Notes:
        兩側座標記得走同一支進位函式，參考 ADR-0012，否則 merge 可能不精準。
    """
    # 1. 指派要查詢的資料表名稱
    table_name = "fact_hourly_weather"

    # 2. 撰寫DQL語句。
    # 刻意不寫成 YEAR(observation_datetime) = :target_year
    # 欄位被函式包住會無法有效運用索引好處 idx_fact_hourly_weather_obt，但資料列是百萬列等級。
    query = f"""SELECT weather_record_id,
                       observation_datetime,
                       longitude_round,
                       latitude_round
                    FROM {table_name}
                        WHERE observation_datetime >= :year_start
                          AND observation_datetime < :next_year_start;
            """

    # 3. 從MySQL server取得資料表
    logger.info(f"Querying TABLE {table_name} FROM DATABASE {database}...")
    df_weather_record = get_table_from_sqlserver(
        query,
        {
            "year_start": f"{target_year}-01-01 00:00:00",
            "next_year_start": f"{target_year + 1}-01-01 00:00:00",
        },
        database=database,
    )
    logger.info(
        f"Finished the query, got {len(df_weather_record)} weather records. "
        f"Start to match them with accidents."
    )

    # 4. 將天氣與事故事實表 join 後取得 weather_record_id
    # 保險措施，再跟 df_all_acc_loc 對齊一次型別與小數點位數
    df_weather_record["longitude_round"] = round_to_weather_grid(
        df_weather_record["longitude_round"]
    )
    df_weather_record["latitude_round"] = round_to_weather_grid(
        df_weather_record["latitude_round"]
    )
    # MySQL 的 DATETIME 讀回來是 datetime 物件，轉成 "YYYY-MM-DD HH:MM:SS" 字串
    # 才與事故側的 approx_accident_datetime 同格式。
    df_weather_record["observation_datetime"] = df_weather_record[
        "observation_datetime"
    ].astype(str)

    df_mrg = df_all_acc_loc.merge(
        df_weather_record,
        how="inner",
        left_on=["approx_accident_datetime", "lon_round", "lat_round"],
        right_on=["observation_datetime", "longitude_round", "latitude_round"],
        suffixes=["_a", "_w"],
    )

    df_mrg = df_mrg.loc[:, ["accident_id", "weather_record_id"]]

    logger.info(
        f"FOR Year {target_year}: {len(df_mrg)}/{len(df_all_acc_loc)} accidents "
        f"were matched to observation records in fact_hourly_weather."
    )
    return df_mrg
