import hashlib
import io
import sys
from datetime import timedelta

import numpy as np
import pandas as pd
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import task

# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到 ./utils下的模組或套件
if "/opt/airflow" not in sys.path:
    sys.path.append("/opt/airflow")

# 2. 在sys.path之後才進行import
from src.util.create_weather_related_tables import create_weather_hist_table
from src.util.mysql_utils import get_pymysql_conn_to_mysql, get_table_from_sqlserver


def e_get_all_acc_geo(target_year: int, *, database: str | None = None) -> pd.DataFrame:
    """Extract: 從MySQL server讀取車禍資料主表，並取得進位後的經緯度組合

    :param target_year: 要從MySQL資料表查詢哪一年份的車禍資料主表
    :type target_year: int
    :param database: 要從MySQL哪一個資料庫查詢target_year車禍資料主表，如不指定，會從預設資料庫查詢
    :type database: str | None = None
    :return: 將經緯度都進位至小數點後二位後得到的pandas DataFrame
    :rtype: DataFrame
    """
    # 1. 指派要查詢的資料表名稱
    table_name = "fact_accident_main"

    # 2. 撰寫DQL語句
    query = f"""SELECT accident_id,
                        TIMESTAMP(date(accident_datetime),
                                    SEC_TO_TIME(ROUND(
                                                        TIME_TO_SEC(
                                                            time(accident_datetime)) / 3600
                                                        ) * 3600
                                                )
                                  ) as `approx_accident_datetime`,
                        longitude,
                        latitude
                    FROM {table_name}
                        WHERE YEAR(accident_datetime) = {target_year}
                        GROUP BY longitude, latitude, accident_id, accident_datetime;
            """

    # 3. 從MySQL server取得資料表
    print(f"Querying TABLE {table_name} FROM DATABASE {database}...")
    df_acc = get_table_from_sqlserver(query, database=database)
    print(
        f"Finished the query! The fetched result contains columns: \n {df_acc.columns}"
    )
    # df_acc: ['accident_id', 'approx_accident_datetime', 'longitude', 'latitude']

    # 4. 經緯度簡化 - 進位
    df_acc["lat_round"] = df_acc["latitude"].astype("float64").round(2)
    df_acc["lon_round"] = df_acc["longitude"].astype("float64").round(2)

    df_acc = df_acc.loc[
        :, ["accident_id", "lat_round", "lon_round", "approx_accident_datetime"]
    ]

    # 7. 轉換成str，在l_load_to_mysql_gcp再次呼叫這支函式時較穩定。
    df_acc["approx_accident_datetime"] = df_acc["approx_accident_datetime"].astype(str)
    print(
        f"FOR Year {target_year}: \nGot {len(df_acc)} locations "
        f"from the TABLE {table_name} containing {len(df_acc)} accidents."
    )

    return df_acc


def t_dataclr_weather_hist(
    df_weather_raw: pd.DataFrame, df_all_acc_loc: pd.DataFrame
) -> pd.DataFrame:
    """Transform: 清理出跟車禍事故日期時間相近的天氣觀測資料、淘汰不相關時間點的冗餘天氣觀測資料。
            最後生成hash確保 觀測日期時間、經度、緯度 的業務語意唯一性。

    :param df_weather_raw: 從OpenMeteo API下載下來的原始整年度天氣觀測資料，為dataframe
    :type df_weather_raw: pd.DataFrame
    :param df_all_acc_loc: 描述每個車禍地點經緯度進位至小數點後二位的結果之dataframe
    :type df_all_acc_loc: pd.DataFrame
    :return: 車禍事故日期時間相近的天氣觀測資料之dataframe，若沒有時間相近的天氣資料，
             則回傳empty dataframe
    :rtype: DataFrame
    """
    # # debug區
    # print("==========清理前==========")
    # print(f"df_all_acc_loc is:{df_all_acc_loc.info()}")
    # print(f"df_all_acc_loc is:{df_all_acc_loc.head()}")
    # print(f"df_weather_raw is:{df_weather_raw.info()}")
    # print(f"df_weather_raw is:{df_weather_raw.head()}")

    # 1. 清理df_weather_raw統一成YYYY-mm-dd HH:MM:SS的格式並先維持字串
    df_weather_raw["datetime_ISO8601"] = (
        df_weather_raw["datetime_ISO8601"]
        .astype(str)
        .str.replace("T", " ")
        .str.replace(":00", ":00:00")
    )

    # 2. 確保兩表的lat_round&lon_round型態一致
    df_all_acc_loc["lat_round"] = df_all_acc_loc["lat_round"].astype("float64").round(2)
    df_all_acc_loc["lon_round"] = df_all_acc_loc["lon_round"].astype("float64").round(2)
    df_weather_raw["latitude_round"] = (
        df_weather_raw["latitude_round"].astype("float64").round(2)
    )
    df_weather_raw["longitude_round"] = (
        df_weather_raw["longitude_round"].astype("float64").round(2)
    )

    # # debug區
    # print("==========清理後，Merge前==========")
    # print(f"df_all_acc_loc is:{df_all_acc_loc.info()}")
    # print(f"df_all_acc_loc is:{df_all_acc_loc.head()}")
    # print(f"df_weather_raw is:{df_weather_raw.info()}")
    # print(f"df_weather_raw is:{df_weather_raw.head()}")

    # 4. 濾掉跟車禍日與車禍地點不相干的資料列 (此舉在幫助將1億筆壓成150萬筆等級的資料列)
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
    # # debug區
    # print("==========Merge後==========")
    # print(f"df_weather_mrg is:{df_weather_mrg.info()}")
    # print(f"df_weather_mrg is:{df_weather_mrg.head()}")

    # 5. 正式置換成寫入資料表所需的欄位名稱
    df_weather_mrg.columns = [
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

    # 6. 生成UK，由於構成業務邏輯唯一的組成有observation_datetime、long_round、lat_round，後二者是浮點數，
    # 浮點數作為SQL中的UK時，需擔心讀取結果跟pandas的讀取結果不同，而發生業務邏輯在寫入前後不一致的問題，所以這裡採用hash value，
    # 從ETL的T層，控管在寫入MySQL前就生好重複性低的hash value當作UK。
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

    # 6-1. 觀望一下hash 後是否有重複的
    dup_cnt = df_weather_mrg["hash_value"].duplicated().sum()
    print(
        f"There are {dup_cnt} rows having duplicated combination of "
        f"observation_datetime, long_round and lat_round."
    )

    # 6-2有可能一筆天氣觀測資料對應到多個事故地點，因此drop_duplicates
    df_weather_mrg = df_weather_mrg.drop_duplicates(subset=["hash_value"])

    # 7. 填補空值
    df_weather_final = df_weather_mrg.where(pd.notnull(df_weather_mrg), None)
    df_weather_final = df_weather_final.replace({np.nan: None})

    # 8. 淘汰不再使用於下游task的變數，以釋放記憶體
    del df_weather_raw, df_weather_mrg
    import gc

    gc.collect()

    print(
        f"Completed data cleaning, got {len(df_weather_final)} rows "
        f"to be loaded to MySQL."
    )
    return df_weather_final


@task
def l_summary_report(target_year: int, upstream) -> str:
    """Load: 載入與確認 - 檢查最終data warehouse (GCS)上面存放的parquet檔案數量

    :param target_year: 要查詢哪一年份的天氣觀測資料儲存區
    :type target_year: int
    :param upstream: 此任務依賴於哪個上游任務之return value(通常是e_crawler_weatherapi() func.)
    :return:  GCS bucke名稱字串
    :rtype: str
    """
    # 1. 找出該年份的所有Parquet之檔案路徑
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")  # 初始化
    bucket_name = "taiwan_traffic_accidents_weather"
    save_dir = f"weather_cache_final/{target_year}/data"
    all_files = gcs_hook.list(bucket_name=bucket_name, prefix=save_dir)
    all_files = [f for f in all_files if f.endswith(".parquet")]

    # 2. 引出總結報告
    print(f"============== Summary: {target_year} ==============")
    print(f"Year: {target_year}")
    print(f"Storage path: {bucket_name}/{save_dir}")
    print(f"Total Parquet files collected: {len(all_files)}")
    print("==============================================")
    return str(bucket_name)


@task(
    retries=2, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(hours=4)
)
def l_transform_and_load_to_mysql(
    target_year: int, *, database: str | None = None, upstream
) -> None:
    """Load: 打開GCS中存好的parquet檔，並清理(T)後直接寫入MySQL。採批次處理，以for-loop按批執行T+L，
    以減緩MySQL連線次數與RAM一次讀取大量資料的壓力。

    :param target_year: 要清理與寫入資料庫的資料所屬年份
    :type target_year: int
    :param database: 打算寫入的資料庫名稱，如不指定(None)，則會寫入預設資料庫
    :type database: str | None = None
    :param upstream: 此任務依賴於哪個上游任務之return value(通常是l_summary_report() func.)
    :return:
    :rtype: None
    """
    # 1. 讀取車禍資料主表，並取得進位的經緯度組合
    df_all_acc_loc = e_get_all_acc_geo(target_year, database=database)

    # 2. 找出該年份的所有Parquet之檔案路徑，如果沒找到檔案就不往下執行。
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")  # 初始化
    bucket_name = "taiwan_traffic_accidents_weather"
    save_dir = f"weather_cache_final/{target_year}/data"
    all_files = gcs_hook.list(bucket_name=bucket_name, prefix=save_dir)
    all_files = [f for f in all_files if f.endswith(".parquet")]

    if not all_files:
        print(f"No weather data found in {save_dir}")
        return None

    # 3. 如果沒有建立過資料表則建立該表
    table_name = "fact_hourly_weather"
    create_weather_hist_table(table_name, database=database)

    # 4. 分批讀取檔案、清理，避免記憶體爆炸或disk I/O壅塞
    try:
        total_rows = 0
        batch_size = Variable.get("WEATHER_FILE_BATCH_SIZE_LOAD_TO_MYSQL", 50)
        print("Preparing to download the parquet files in multiple batches...")
        for i in range(0, len(all_files), batch_size):
            # 4-1. 讀取50個檔案後合併在一個大dataframe
            batch_files = all_files[i : i + batch_size]
            df_list = []  # 將讀取的df收在一個清單
            print(
                f"Downloading the {batch_size} files from GCS "
                f"as batch No: {i // batch_size}"
            )
            for f in batch_files:
                file_data = gcs_hook.download(bucket_name=bucket_name, object_name=f)
                df_w_chunk = pd.read_parquet(io.BytesIO(file_data))

                # 以防萬一如果E step出現存寫錯誤，這裡還可以檢查一次。
                if "datetime_ISO8601" not in df_w_chunk.columns:
                    print(
                        f"Warning：Cannot found the necessary column name "
                        f"'datetime_ISO8601' in the file, {f}.\n"
                        f"Skipped cleaning this file."
                    )
                    continue

                df_list.append(df_w_chunk)

            # 4-2. 清單製備好後一口氣合併，若打開過的parquet檔案都是格式錯誤，則直接讀另外50個檔案
            print(
                f"Finished downloading and got {len(df_list)} effective parquet files"
            )
            if not df_list:
                continue
            df_weather_raw = pd.concat(df_list, ignore_index=True)

            # 4-3.合併後開始清洗
            print("Start data cleaning......")
            df_transformed = t_dataclr_weather_hist(df_weather_raw, df_all_acc_loc)

            # 5. 準備INSERT資料表時需要的SQL語句，採用UPSERT
            # 先準備INSERT部分
            columns = ", ".join(df_transformed.columns)
            placeholders = ", ".join(["%s"] * len(df_transformed.columns))

            # 準備UPDATE的部分：更新hash value以外的參數。
            columns_list = df_transformed.columns.tolist()
            update_part = ", ".join(
                [f"{col}=VALUES({col})" for col in columns_list if col != "hash_value"]
            )

            # 組合出SQL語句
            dml_str = f"""
                            INSERT INTO {table_name} ({columns})
                            VALUES ({placeholders})
                            ON DUPLICATE KEY UPDATE {update_part}
                        """

            # 6. 寫入資料表
            print(f"Inserting into MySQL TABLE {table_name}....")
            conn = get_pymysql_conn_to_mysql(database)
            cursor = conn.cursor()
            cursor.executemany(dml_str, df_transformed.values.tolist())
            conn.commit()
            total_rows += len(df_transformed)
            print(
                f"Batch {i // batch_size} finished: Inserted {len(df_transformed)} rows, Total so far: {total_rows}"
            )

            # 7. 手動釋放該檔案佔用的記憶體
            del df_weather_raw, df_transformed, df_list
            import gc

            gc.collect()
    except Exception as e:
        print(
            f"Error on Batch No {i // batch_size}, Error msg: {e}. "
            f"In this batch, failed to load the following files to MySQL:"
            f"\n{batch_files}"
        )
        conn.rollback()
    else:
        print(f"Successfully loaded {target_year} data to MySQL.")
        return f"{target_year}_load_done"
    finally:
        # 8. 關閉cursor與conn
        cursor.close()
        conn.close()
