"""Load 階段：分批把 GCS 上的天氣觀測清洗後 upsert 進 `fact_hourly_weather`。"""

import math

import pandas as pd

from src.task.create_weather_tables import create_weather_tables
from src.task.e_crawling_weather import (
    MAX_FAILURE_RATE,
    WEATHER_BUCKET,
    e_get_all_acc_geo,
    weather_data_prefix,
)
from src.task.t_fact_hourly_weather import t_fact_hourly_weather
from src.util import gcs_utils
from src.util.logger_crtx import get_logger
from src.util.mysql_utils import get_engine_to_mysql, upsert_to_table

logger = get_logger(__name__)


def l_fact_hourly_weather(
    target_year: int,
    *,
    database: str | None = None,
    batch_size: int = 50,
) -> None:
    """分批讀取 GCS 上該年度的天氣觀測，清洗後寫入 `fact_hourly_weather`。

    因為整年的天氣資料是數百萬列，一次讀進對記憶體可能有壓力，
    所以本函式採用分批讀取。主要流程是：
    - 先取得該年度所有事故的座標與時間。
    - 列出 GCS 上已經下載好的觀測點 Parguet 檔案，一個檔案都沒有則直接 raise，
    - 再每 `batch_size` 個 Parguet 檔案打開合併成一批，清洗後 upsert。

    缺欄的損壞檔案會被跳過並記 warning，最後結算比例，採少量容忍 20%，超過則 raise。

    Args:
        target_year (int): 要清洗並寫入的年份。
        database (str | None): 目標資料庫名稱；不指定則寫入預設資料庫。
        batch_size (int): 一批合併幾個 Parquet 檔案再清洗，預設 50。

    Raises:
        RuntimeError: 該年度在 GCS 上找不到任何檔案，或損壞檔案的比例超過 20%。
        GoogleAPIError: 列出或讀取 GCS 物件失敗。
        pymysql.MySQLError: 任一批寫入失敗，該批事務復原後往外拋。

    Notes:
        不把故障吞成空結果參考 ADR-0003。
    """
    # 1. 讀取 fact_accident_main，取得進位後的座標與整點化時間
    df_all_acc_loc = e_get_all_acc_geo(target_year, database=database)

    # 2. 找出 GCS 上面，指定年份的所有 Parquet 物件路徑
    save_dir = weather_data_prefix(target_year)
    all_files = gcs_utils.list_parquet(bucket=WEATHER_BUCKET, prefix=save_dir)

    # 若 GCS 上沒有檔案，代表可能上游的 e_* task 有問題，必須排查
    if not all_files:
        raise RuntimeError(
            f"{target_year} 年在 {WEATHER_BUCKET}/{save_dir} 下找不到任何 Parquet 檔案，"
            f"上游的抓取階段可能未產出資料"
        )

    batch_count = math.ceil(len(all_files) / batch_size)
    logger.info(
        f"{target_year} 年共 {len(all_files)} 個觀測點檔案，分 {batch_count} 批寫入"
    )

    # 3. 建立天氣資料事實表
    create_weather_tables(get_engine_to_mysql(database))

    # 4. 分批讀取 Parquet 檔案、清理、寫入，避免一次把整年資料讀進記憶體
    total_rows = 0
    skipped_files = 0

    for i in range(0, len(all_files), batch_size):
        batch_files = all_files[i : i + batch_size]
        batch_no = i // batch_size
        logger.info(f"Downloading batch {batch_no}（{len(batch_files)} 個檔案）")

        df_list = []
        for f in batch_files:
            df_w_chunk = gcs_utils.read_parquet(bucket=WEATHER_BUCKET, object_name=f)

            # 這些 Parquet 的欄位是 e_* task 自己訂的，缺欄代表當時寫壞了，非 API 問題。
            if "datetime_ISO8601" not in df_w_chunk.columns:
                logger.warning(f"{f} 缺少 datetime_ISO8601 欄位，跳過")
                skipped_files += 1
                continue

            df_list.append(df_w_chunk)

        if not df_list:
            logger.warning(f"Batch {batch_no} 的檔案全數無法使用，略過本批")
            continue

        df_weather_raw = pd.concat(df_list, ignore_index=True)

        # 5. 與 step 1 的表做 join 濾出真正需要的資料列。
        df_transformed = t_fact_hourly_weather(df_weather_raw, df_all_acc_loc)

        # hash_value 是唯一鍵，衝突時更新其餘欄位
        upsert_to_table(
            df_transformed,
            table="fact_hourly_weather",
            update_columns=[
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
            ],
            database=database,
        )

        total_rows += len(df_transformed)
        logger.info(
            f"Batch {batch_no} finished: inserted {len(df_transformed)} rows, "
            f"total so far {total_rows}"
        )

    # 6. 結算損壞檔案的比例
    failure_rate = skipped_files / len(all_files)
    if failure_rate > MAX_FAILURE_RATE:
        raise RuntimeError(
            f"{target_year} 年有 {skipped_files}/{len(all_files)} 個檔案格式錯誤，"
            f"比例過高，抓取階段可能出過問題"
        )
    if skipped_files:
        logger.warning(f"{target_year} 年跳過 {skipped_files}/{len(all_files)} 個檔案")

    logger.info(f"Successfully loaded {target_year} data to MySQL：{total_rows} 列")
