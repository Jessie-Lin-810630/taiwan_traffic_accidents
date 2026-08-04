import random
import sys
import time
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow.exceptions import AirflowException
from airflow.sdk import task

# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到 ./utils下的模組或套件
if "/opt/airflow" not in sys.path:
    sys.path.append("/opt/airflow")

# 2. 在sys.path之後才進行import
from src.util import gcs_utils
from src.util.mysql_utils import get_table_from_sqlserver
from src.util.request_weather_api import request_weather_api

# 天氣觀測資料在 GCS 上的落點。bucket 與路徑組法是本 pipeline 的慣例，
# 不屬於 gcs_utils —— 換一個 bucket 或換一套路徑，那支工具仍然成立。
WEATHER_BUCKET = "taiwan_traffic_accidents_weather"


def _year_prefix(target_year: int) -> str:
    """該年度天氣資料在 GCS 上的根前綴。"""
    return f"weather_cache_final/{target_year}"


def _batch_object(target_year: int, batch_id: int) -> str:
    """批次經緯度清單的暫存物件路徑。

    這份暫存檔的存在理由是 Airflow 的 dynamic task mapping：
    `expand()` 只適合傳純量，DataFrame 走 XCom 會過長，
    因此以批號為鍵、把 DataFrame 放在 GCS 上交接。
    """
    return f"{_year_prefix(target_year)}/tmp/batch_no_{batch_id}.parquet"


def weather_data_prefix(target_year: int) -> str:
    """該年度天氣觀測結果的前綴。"""
    return f"{_year_prefix(target_year)}/data"

"""========================定義TASK 1========================"""


@task(
    retries=3,
    retry_delay=timedelta(minutes=10),
    execution_timeout=timedelta(minutes=30),
)
def e_get_uniq_acc_geo(
    target_year: int, *, database: str | None = None
) -> pd.DataFrame:
    """Extract: 從MySQL server讀取車禍資料主表，並取得進位＋去重後的經緯度組合

    :param target_year: 要從MySQL資料表查詢哪一年份的車禍資料主表
    :type target_year: int
    :param database: 要從MySQL哪一個資料庫查詢target_year車禍資料主表，如不指定，會從預設資料庫查詢
    :type database: str | None = None
    :return: 將經緯度都進位至小數點後二位，再去掉重複出現的經緯度組合之後的pandas DataFrame
    :rtype: DataFrame
    """
    # 1. 指派要查詢的資料表名稱
    table_name = "fact_accident_main"

    # 2. 撰寫DQL語句
    query = f"""SELECT longitude, latitude
                    FROM {table_name}
                        WHERE YEAR(accident_datetime) = {target_year}
                            GROUP BY longitude, latitude;
            """

    # 3. 從MySQL server取得資料表
    print(f"Querying TABLE {table_name} FROM DATABASE {database}...")
    df_acc = get_table_from_sqlserver(query, database=database)
    print(
        f"Finished the query! The fetched result contains columns: \n {df_acc.columns}"
    )

    # 4. 經緯度簡化 - 進位
    # 在WGS84座標系下，經緯度差0.01度大約相當於緯度方向1110公尺、經度方向約1000公尺。
    # 一般天氣模型網格解析度為1~20公里，0.01度差異(約1公里)在同一網格內，對觀測結果影響很小。
    # 所以將經緯度統一進位到小數點後2位，減少API請求次數與後續要處理的資料量。
    df_acc["lat_round"] = df_acc["latitude"].astype("float64").round(2)
    df_acc["lon_round"] = df_acc["longitude"].astype("float64").round(2)

    # 5. 進位後可能發生相同經緯度組合，故保險起見做去重
    df_acc_uniq_loc = df_acc.drop_duplicates(["lat_round", "lon_round"])

    # 6. 只留'lat_round', 'lon_round'這二欄，避免造成Returned value、x-com過長
    df_acc_uniq_loc = df_acc_uniq_loc.loc[:, ["lat_round", "lon_round"]]

    print(
        f"FOR Year {target_year}: \nGot {len(df_acc_uniq_loc)} unique locations "
        f"from the TABLE {table_name} containing {len(df_acc)} accidents."
    )

    return df_acc_uniq_loc


"""========================定義TASK 2========================"""


@task
def prep_batch_plan(
    df_acc_unique_loc: pd.DataFrame, target_year: int, batch_size=50
) -> list[int]:
    """將DataFrame: df_acc_unique_loc中的經緯度組合與GCS既有的天氣觀測資料比對，
    盤點有哪些經緯度組合的年度天氣觀測資料尚未下載&尚未存放於GCS。
    過濾並留下"缺失天氣觀測資料的經緯度組合"它所屬的資料列，然後以50列為一批次單位，
    將資料量切塊至"50列/dataframe"後，另存於GCS的tmp路徑下。

    :param df_acc_unique_loc: 經過進位與去重而得到的事故地經緯度
    :type df_acc_unique_loc: pd.DataFrame
    :param target_year: 要從GCS查詢哪一年份的天氣觀測資料
    :type target_year: int
    :param batch_size: 幾個資料列為一批次，預設值為50列(即50個獨特的經緯度組合)
    :return: 裝有多個批號的list，每一元素為一個批號。
    :rtype: list[int]
    """
    # 1. 取出經緯度資訊，定義出一個經緯度地點的氣象觀測資料的檔案名稱
    df_acc_uniq_loc = df_acc_unique_loc.copy()
    df_acc_uniq_loc["file_name"] = (
        df_acc_uniq_loc["lat_round"].astype(str).str.replace(".", "-", regex=False)
        + "_"
        + df_acc_uniq_loc["lon_round"].astype(str).str.replace(".", "-", regex=False)
        + ".parquet"
    )

    # 2. 查詢目前GCS的data路徑下面有幾份.parquet檔案，並存成set、可以減少後續找元素時的時間複雜度
    print(f"Searching the existing files in {weather_data_prefix(target_year)}...")
    files = gcs_utils.list_parquet(WEATHER_BUCKET, weather_data_prefix(target_year))
    existing_files = {Path(f).name for f in files}

    # 3. 針對歷史年份，用~排除同名檔案。針對尚未結束的今年，不排除同名檔案。
    this_year = pendulum.now().year
    if target_year < this_year:
        df_needed = df_acc_uniq_loc[~df_acc_uniq_loc["file_name"].isin(existing_files)]
        print(
            f"FOR Year {target_year}: \nThere are {len(existing_files)} files already existing on GCS. "
            f"Therefore, {len(df_needed)} locations without saved weather data should be extra collected."
        )
    else:
        df_needed = df_acc_uniq_loc.copy()
        print(
            f"FOR Year {target_year}: \nThere are {len(existing_files)} files already existing on GCS. "
            f"However, these files will be repeatedly collected and overwriten to include "
            f"more 3 days of weather data since last saving."
        )

    # 4. 制訂batch plan，並將每一batch的dataframe存到GCS的tmp路徑下，對應批號則存到batch plan變數。
    batch_plan = []

    for i in range(0, len(df_needed), batch_size):
        df_a_batch = df_needed.iloc[i : i + batch_size].copy()
        batch_id = i // batch_size
        df_a_batch["batch_id"] = batch_id

        # index=True 是本呼叫點特有的：暫存檔要能還原成與切片當下相同的 DataFrame。
        gcs_utils.write_parquet(
            WEATHER_BUCKET,
            _batch_object(target_year, batch_id),
            df_a_batch,
            index=True,
        )

        # 存下對應批號
        batch_plan.append(batch_id)

    print(
        f"FOR Year {target_year}: \n, there are {len(batch_plan)} batches "
        f"that we will call for weather API by the sub-tasks."
    )

    del df_acc_uniq_loc, df_acc_unique_loc
    import gc

    gc.collect()
    return batch_plan


""" == == == == == == == == == == == ==定義TASK 3 == == == == == == == == == == == =="""


@task(
    pool="weather_api_pool",  # 需到UI進一步給值，指一次可以執行多少個同類task
    retries=3,  # 如果出現except，最多再重試3次，總計task group中，每個task可跑4次
    retry_delay=timedelta(minutes=20),  # 20分鐘後才重試
    retry_exponential_backoff=True,  # 讓等待時間隨次數增加(指數退避)
    max_retry_delay=timedelta(hours=2),  # 指數退避下，最長間隔2小時後重試
    # 排除等待時間，如果執行總時間超過2小時，殺掉該task避免佔用pool資源
    execution_timeout=timedelta(hours=2),
    do_xcom_push=False,  # 回傳的xcom不推送到下一個task，省掉存xcom的記憶體空間
)
def e_crawler_weatherapi(batch_id: int, target_year: int) -> str | None:
    """Extract: 遍歷引入的batch_no找到開啟對應的df_one_batch，擷取當中的經緯度欄位，
    向OpenMeteo historical weather API請求該地點的某個整年度天氣觀測資料。

    :param batch_id: 批號
    :type batch_id: int
    :param target_year: 說明向OpenMeteo historical weather API請求哪一年度的天氣觀測資料
    :type target_year: int
    :return: GCS bucke名稱字串，若request API失敗或觸發例外則回傳None
    :rtype: str | None
    """
    print(f"Processing batch no {batch_id}......")
    # 1. 從GCS上打開df_one_batch
    df_one_batch = gcs_utils.read_parquet(
        WEATHER_BUCKET, _batch_object(target_year, batch_id)
    )
    df_one_batch["lat_round"] = df_one_batch["lat_round"].astype("float64").round(2)
    df_one_batch["lon_round"] = df_one_batch["lon_round"].astype("float64").round(2)

    # 3. Calling for API時，需要將經緯度以字串形式傳入參數，故遍歷df_one_batch把經度、緯度分別組合出一組字串
    lat_round_lst = [str(lat_num) for lat_num in df_one_batch["lat_round"]]
    lon_round_lst = [str(lon_num) for lon_num in df_one_batch["lon_round"]]
    lats_str = ",".join(lat_round_lst)
    lons_str = ",".join(lon_round_lst)

    # 3. Calling for API時，需要傳入日期區間作為參數，故準備start_date＆end_date兩字串
    start_date = f"{target_year}-01-01"
    # 如果當年度尚未結束，但end_date設定為yyyy-12-31再作為參數傳入的話，API會回傳None，因此需要彈性指派end_date的值
    today = pendulum.now()
    previous_day = today.subtract(days=3).format("YYYY-MM-DD")
    this_year = today.year
    end_date = f"{target_year}-12-31" if this_year > target_year else f"{previous_day}"

    # 4. Calling for API時，需要傳入氣象指標
    vars = [
        "temperature_2m",
        "apparent_temperature",
        "rain",
        "precipitation",
        "weather_code",
        "wind_speed_10m",
        "wind_gusts_10m",
    ]

    # 5. Calling for API
    data = request_weather_api(lats_str, lons_str, start_date, end_date, vars)

    # 6. 將請求結果data做簡易清洗後就立即存成parquet，備份資料源。
    if data:
        data = data if isinstance(data, list) else [data]
        print("Received the response from API.")
        # 追蹤寫入失敗的檔案數量
        failed_files = 0
        for j in range(0, len(data)):  # len(data) == len(lat_round_lst)
            if "hourly" in data[j]:  # data[j] is a dict of one certain location
                # data[j]["hourly"] is also a dict
                df_a_loc_hourly = pd.DataFrame(data[j]["hourly"])

                # 補回lat_round、lon_round，以便後續追溯這筆天氣資料是靠近哪一個事故地經緯度。
                df_a_loc_hourly["latitude_round"] = float(lat_round_lst[j])
                df_a_loc_hourly["longitude_round"] = float(lon_round_lst[j])

                # 置換成想要的欄位名稱
                # 但盡可能的跟response中欄位名稱一樣，此步驟能做到與API端口隔離就好。
                col_name = [
                    "datetime_ISO8601",
                    "temperature_2m_degree",
                    "apparent_temperature_degree",
                    "rain_mm",
                    "precipitation_mm",
                    "weather_code",
                    "wind_speed_10m_km_per_h",
                    "wind_gusts_10m_km_per_h",
                    "latitude_round",
                    "longitude_round",
                ]
                df_a_loc_hourly.columns = col_name

                # 定義存檔名稱
                lat_s = str(lat_round_lst[j]).replace(".", "-")
                lon_s = str(lon_round_lst[j]).replace(".", "-")
                file_name = (
                    f"{weather_data_prefix(target_year)}/"
                    f"cralwer_batch_{batch_id}_{lat_s}_{lon_s}.parquet"
                )

                # 存成 Parquet 較節省空間(直接存到GCS上)，印出進度
                try:
                    print(f"Saving the {(j + 1)}/{len(data)} file....")

                    gcs_utils.write_parquet(WEATHER_BUCKET, file_name, df_a_loc_hourly)
                except Exception as e:
                    failed_files += 1  # 失敗就累計1
                    print(f"Failed to save the file!\nError msg:{e}")
                else:
                    print(f"Successfully saved the file, {file_name}!")
        print(f"Finished the batch {batch_id}!")

        # 迴圈結束後結算失敗率
        if failed_files / len(data) > 0.2:
            # 太高則raise Exception，task會進入重試機制
            raise AirflowException(
                f"Alarm: Failure rate was too high: {failed_files}/{len(data)} failed"
            )
        elif failed_files / len(data) > 0:
            print(f"Warning: Partially failure on {failed_files}/{len(data)} files.")
    else:
        # 若data是Falthy value，印出前100個字看發生什麼事
        print(f"API return Data as: {str(data)[:100]}")

    # 7. sleep緩解server負擔
    time.sleep(random.uniform(5, 10))
    return WEATHER_BUCKET
