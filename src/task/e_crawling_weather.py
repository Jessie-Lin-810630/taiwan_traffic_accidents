"""Extract 階段：向 OpenMeteo 取得事故地點的逐小時天氣觀測，落地為 GCS Parquet。"""

import random
import time
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from tenacity import retry, retry_if_exception, stop_after_attempt

from src.util import gcs_utils
from src.util.crawling_utils import RETRY_ATTEMPTS, RETRY_WAIT, is_transient
from src.util.logger_crtx import get_logger
from src.util.mysql_utils import get_table_from_sqlserver

logger = get_logger(__name__)

# 天氣觀測資料在 GCS 上的落點。bucket 與路徑組法是本 pipeline 的慣例，
# 不屬於 gcs_utils —— 換一個 bucket 或換一套路徑，那支工具仍然成立。
WEATHER_BUCKET = "taiwan_traffic_accidents_weather"

# 算「今年」與「三天前」都用台北時區 —— 與 API 請求的 timezone 參數一致。
# 容器沒有設定 TZ，若取本地時間會是 UTC，與要來的資料差 8 小時。
TAIPEI = ZoneInfo("Asia/Taipei")

# OpenMeteo 歷史天氣 API（站台專屬，因此留在本模組而非 crawling_utils）。
# 參考文件：https://open-meteo.com/en/docs/historical-weather-api
OPENMETEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

WEATHER_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "rain",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
]

# 經緯度進位的網格大小，單位為度（ADR-0012）。
GRID_STEP = 0.05

# 請求時一律指定的高程，單位為公尺（ADR-0012）。
# 不指定的話 OpenMeteo 會依每個座標各自的海拔調整氣溫，
# 同一個網格內的座標因此拿到不同的值，進位就失去意義。
FIXED_ELEVATION_M = 10

# 寫入失敗率超過此比例即視為該批次失敗。
MAX_FAILURE_RATE = 0.2

# OpenMeteo 歷史觀測的延遲天數：今天往前 3 天的資料才查得到。
# 這個延遲是「一個月何時算抓完」的判準，見 `is_month_complete()`（ADR-0013）。
API_LAG_DAYS = 3


def round_to_weather_grid(series: pd.Series) -> pd.Series:
    """將經緯度進位到氣象網格（ADR-0012）。

    事故資料與天氣資料最後要 merge，兩邊的經緯度都必須用這支函式算出來。
    只要有一邊用了別的算法，join 會一列都對不上，而且不會報錯。

    末尾的 `.round(2)` 是必要的：`24.13 / 0.05` 取整後乘回去，浮點運算可能
    得到 `24.150000000000002`，而 hash 是拿字串算的（見 `t_fact_hourly_weather`），
    多出來的尾數會讓兩邊算出不同的 hash。

    Parameters:
        series (pandas.Series): 原始的經度或緯度。

    Returns:
        pandas.Series: 進位到 `GRID_STEP` 網格後的值。
    """
    scaled = series.astype("float64") / GRID_STEP
    return (scaled.round() * GRID_STEP).round(2)


def _year_prefix(target_year: int) -> str:
    """該年度天氣資料在 GCS 上的根前綴。"""
    return f"weather_cache_final/{target_year}"


def _batch_object(target_year: int, month: int, batch_id: int) -> str:
    """批次經緯度清單的暫存物件路徑。

    這份暫存檔的存在理由是 Airflow 的 dynamic task mapping：
    `expand_kwargs()` 只適合傳純量，DataFrame 走 XCom 會過長，
    因此以（月, 批號）為鍵、把 DataFrame 放在 GCS 上交接。

    路徑必須帶月份 —— 不同月的同號批次否則會互相覆蓋（ADR-0013）。
    """
    return (
        f"{_year_prefix(target_year)}/tmp/"
        f"{target_year}-{month:02d}/batch_no_{batch_id}.parquet"
    )


def weather_data_prefix(target_year: int, month: int | None = None) -> str:
    """天氣觀測結果的 GCS 前綴；給了月份就縮到該月。

    月份放進路徑，是為了讓「GCS 上有哪些檔案」本身就是完整的抓取進度 ——
    不需要第二份狀態，也不需要下載檔案內容才知道涵蓋到哪一天（ADR-0013）。

    Parameters:
        target_year (int): 年份。
        month (int | None): 月份，1～12。不指定則回傳涵蓋整年的前綴 ——
            L 階段列整年的檔案時用它（各月子路徑靠 GCS 的遞迴列舉一併帶出）。

    Returns:
        str: 不含 bucket 名稱的物件路徑前綴。
    """
    prefix = f"{_year_prefix(target_year)}/data"
    return prefix if month is None else f"{prefix}/{target_year}-{month:02d}"


def blob_file_name(lat: float, lon: float) -> str:
    """一個觀測點的 Parquet 檔名。

    `prep_batch_plan()` 用它組出「該有哪些檔案」，`e_crawler_weatherapi()`
    用它決定「要存成什麼檔名」—— 兩處必須是同一支函式。

    ADR-0013 之前兩處各自組字串且對不上（存檔多了 `cralwer_batch_{批號}_` 前綴），
    因此 `isin(existing_files)` 恆為 False，續跑的排除從未生效過。
    檔名也不再帶批號 —— 批號隨每次 run 的待抓清單浮動，同一個觀測點會落在
    不同批號而產生重複檔案。
    """
    return f"{str(lat).replace('.', '-')}_{str(lon).replace('.', '-')}.parquet"


def _month_last_day(target_year: int, month: int) -> date:
    """該月最後一天。"""
    return date(target_year, month, monthrange(target_year, month)[1])


def _latest_available_date_from_api(today: date) -> date:
    """OpenMeteo 目前查得到的最新日期。"""
    return today - timedelta(days=API_LAG_DAYS)


def is_month_complete(target_year: int, month: int, today: date) -> bool:
    """該月是否已經抓得完整，之後不必再重抓。

    判準是「該月最後一天已落在 API 查得到的範圍內」，**不是**「該月已經過去」。

    用後者會讓每個月的最後 3 天永遠抓不到：1/31 的 run 只請求得到 1/28，
    2/03 的 run 若認為一月已過去就會跳過它，1/29～1/31 因此永久缺失，
    而且檔案在、載入照常，完全看不出來（ADR-0013 決策三）。

    Parameters:
        target_year (int): 年份。
        month (int): 月份，1～12。
        today (date): 今天的日期（台北時區）。

    Returns:
        bool: 該月是否已抓完整。
    """
    return _month_last_day(target_year, month) <= _latest_available_date_from_api(today)


def months_to_fetch(target_year: int, today: date) -> list[int]:
    """該年度有哪些月份需要抓取。

    以「該月一號已落在 API 查得到的範圍內」為準，因此尚未開始的月份不會入列。
    未來年份會得到空 list。

    Parameters:
        target_year (int): 年份。
        today (date): 今天的日期（台北時區）。

    Returns:
        list[int]: 需要抓取的月份，由小到大。
    """
    latest = _latest_available_date_from_api(today)
    return [m for m in range(1, 13) if date(target_year, m, 1) <= latest]


def month_date_range(target_year: int, month: int, today: date) -> tuple[str, str]:
    """該月要向 API 請求的起訖日期，格式 `YYYY-MM-DD`。

    迄日取「該月最後一天」與「API 查得到的最新日期」之中較早的那個 ——
    當月尚未結束時請求到月底，API 會回傳 None。

    Parameters:
        target_year (int): 年份。
        month (int): 月份，1～12。
        today (date): 今天的日期（台北時區）。

    Returns:
        tuple[str, str]: (起日, 迄日)。
    """
    start = date(target_year, month, 1)
    end = min(
        _month_last_day(target_year, month), _latest_available_date_from_api(today)
    )
    return start.isoformat(), end.isoformat()


def _should_retry(exc: BaseException) -> bool:
    """判斷是否值得重試；429 刻意排除（ADR-0006）。

    OpenMeteo 免費層有三個限流窗口 —— 每分鐘 600、每小時 5,000、每日 10,000 ——
    且呼叫次數是**加權**的：成本隨時間區間、地點數、變數數上升，以小數計。

    2026-08-05 實跑 d07 量到的換算是 **1 次額度 ≈ 25 個「觀測點 × 天」**
    （一批 50 個觀測點 × 214 天約 526 次）。被 429 擋掉的請求也計入額度。
    先綁定的是**每小時**額度，不是每日。

    這意味著 429 需要等待的尺度是分鐘到小時，而 `RETRY_WAIT` 只有 2、4 秒 ——
    在這裡重試必然徒勞，只會在 log 裡留下三筆永遠不會成功的紀錄。
    跨過小時窗口的是 Airflow task 層的重試（20 → 40 → 80 分）；
    跨過日窗口的則是下一次 DAG run —— `prep_batch_plan()` 會排除
    GCS 上已完成的（觀測點, 月），自動從斷點續跑（ADR-0013）。

    其餘暫時性故障（連線中斷、逾時、5xx）則相反 —— 幾秒的退避往往就能自癒，
    在此重試可以省下一輪 Airflow task 層的重試（間隔 20 分鐘起跳）。
    """
    response = getattr(exc, "response", None)
    if response is not None and response.status_code == 429:
        return False
    return is_transient(exc)


retry_on_transient_weather = retry(
    retry=retry_if_exception(_should_retry),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=RETRY_WAIT,
    reraise=True,  # 重試耗盡時拋出原始例外，而非 tenacity 的 RetryError
)


@retry_on_transient_weather
def _request_weather_api(
    lats_str: str,
    lons_str: str,
    start_date: str,
    end_date: str,
    variables: list[str],
) -> list[dict]:
    """向 OpenMeteo 請求一批地點的逐小時天氣觀測。

    回傳一律正規化為 list：OpenMeteo 在單一地點時回傳 dict、多地點時回傳 list，
    那是 API 的特性，不該讓呼叫端知道（`batch_size=1` 時才會踩到的邊界）。

    Parameters:
        lats_str (str): 以逗號分隔的緯度字串。
        lons_str (str): 以逗號分隔的經度字串，順序需與 `lats_str` 對應。
        start_date (str): 起始日期，格式 `YYYY-MM-DD`。
        end_date (str): 結束日期，格式 `YYYY-MM-DD`。
        variables (list[str]): 要取得的氣象指標。

    Returns:
        list[dict]: 每個元素是一個地點的回應，順序與請求的經緯度一致。

    Raises:
        RuntimeError: 回應不含任何地點資料。請求了地點卻拿回空的，
            是故障而非「查無資料」，不可吞成正常回傳（ADR-0003）。
        requests.exceptions.HTTPError: 4xx／5xx；其中 5xx 已重試過，
            429 刻意不重試（見 `_should_retry`）。
        requests.exceptions.Timeout | ConnectionError: 重試耗盡後原樣拋出。
    """
    # 高程要逐地點指定，數量必須與經緯度一致（ADR-0012）
    location_count = len(lats_str.split(","))
    params = {
        "latitude": lats_str,
        "longitude": lons_str,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": variables,
        "timezone": "Asia/Taipei",
        "elevation": ",".join([str(FIXED_ELEVATION_M)] * location_count),
    }

    logger.info(f"向 OpenMeteo 請求 {start_date} ~ {end_date} 的天氣觀測")

    try:
        response = requests.get(OPENMETEO_ARCHIVE_URL, params=params, timeout=120)
        # 非 2xx 一律在此拋出，3xx 也不再穿過所有分支隱式回傳 None
        response.raise_for_status()

    except requests.exceptions.HTTPError as exc:
        # 429 不會被重試，是確定的失敗；5xx 仍可能自癒，記 warning（ADR-0006）
        if exc.response is not None and exc.response.status_code == 429:
            logger.error(
                "OpenMeteo 回報 429（額度用盡）。"
                "限流窗口為分／時／日，重試無用；"
                "下一次 DAG run 會由 prep_batch_plan 自動續跑未完成的批次。"
            )
        else:
            logger.warning(f"OpenMeteo HTTP 回應異常：{exc}（將視情況重試）")
        raise

    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
        logger.warning(f"OpenMeteo 連線失敗：{exc}（將視情況重試）")
        raise

    except Exception:
        logger.error("OpenMeteo 請求發生未預期的錯誤")
        raise

    payload = response.json()
    records = payload if isinstance(payload, list) else [payload]

    if not records:
        raise RuntimeError(
            f"OpenMeteo 回應不含任何地點資料（{start_date} ~ {end_date}）"
        )

    logger.info(f"收到 OpenMeteo 回應，共 {len(records)} 個地點")
    return records


"""========================定義TASK 1========================"""


def e_get_uniq_acc_geo(
    target_year: int, *, database: str | None = None
) -> pd.DataFrame:
    """Extract: 從MySQL server讀取車禍資料主表，並取得進位＋去重後的經緯度組合

    :param target_year: 要從MySQL資料表查詢哪一年份的車禍資料主表
    :type target_year: int
    :param database: 要從MySQL哪一個資料庫查詢target_year車禍資料主表，如不指定，會從預設資料庫查詢
    :type database: str | None = None
    :return: 將經緯度都進位至氣象網格，再去掉重複出現的經緯度組合之後的pandas DataFrame
    :rtype: DataFrame
    """
    # 1. 指派要查詢的資料表名稱
    table_name = "fact_accident_main"

    # 2. 撰寫DQL語句。年份走 bind parameter，不內插（ADR-0009）
    query = f"""SELECT longitude, latitude
                    FROM {table_name}
                        WHERE CAST(LEFT(accident_id, 4) AS SIGNED) = :target_year
                            GROUP BY longitude, latitude;
            """

    # 3. 從MySQL server取得資料表
    logger.info(f"Querying TABLE {table_name} FROM DATABASE {database}...")
    df_acc = get_table_from_sqlserver(
        query, {"target_year": target_year}, database=database
    )
    logger.info(
        f"Finished the query! The fetched result contains columns: \n {df_acc.columns}"
    )

    # 4. 經緯度進位到氣象網格
    # OpenMeteo 背後的模式把地表切成約 0.07 度的格子，同一格內的座標拿到的是
    # 同一份觀測值。進位到比它略細的 0.05 度，可讓落在同一格的事故共用一次
    # API 請求，大幅減少請求次數（ADR-0012）。
    df_acc["lat_round"] = round_to_weather_grid(df_acc["latitude"])
    df_acc["lon_round"] = round_to_weather_grid(df_acc["longitude"])

    # 5. 進位後可能發生相同經緯度組合，故保險起見做去重
    df_acc_uniq_loc = df_acc.drop_duplicates(["lat_round", "lon_round"])

    # 6. 只留'lat_round', 'lon_round'這二欄，避免造成Returned value、x-com過長
    df_acc_uniq_loc = df_acc_uniq_loc.loc[:, ["lat_round", "lon_round"]]

    logger.info(
        f"FOR Year {target_year}: \nGot {len(df_acc_uniq_loc)} unique locations "
        f"from the TABLE {table_name} containing {len(df_acc)} accidents."
    )

    return df_acc_uniq_loc


def e_get_all_acc_geo(target_year: int, *, database: str | None = None) -> pd.DataFrame:
    """Extract: 讀取車禍資料主表，取得每一筆事故的進位座標與整點化時間。

    與 `e_get_uniq_acc_geo()` 的差別在**粒度**：那支去重後只回傳觀測點清單
    （約數百列），供決定要向 API 請求哪些地點；本函式一筆事故一列
    （約數十萬列），供 `t_fact_hourly_weather()` 與天氣資料 merge。

    兩支都呼叫 `round_to_weather_grid()`，merge 才接得起來。

    :param target_year: 要從MySQL資料表查詢哪一年份的車禍資料主表
    :type target_year: int
    :param database: 要從MySQL哪一個資料庫查詢target_year車禍資料主表，如不指定，會從預設資料庫查詢
    :type database: str | None = None
    :return: 將經緯度都進位至氣象網格後得到的pandas DataFrame
    :rtype: DataFrame
    """
    # 1. 指派要查詢的資料表名稱
    table_name = "fact_accident_main"
    table_name_to_join = "dim_accident_day"

    # 2. 撰寫DQL語句。年份走 bind parameter，不內插（ADR-0009）
    query = f"""SELECT t1.accident_id,
                       concat(t2.accident_date,
                              "T",
                              (SEC_TO_TIME(ROUND(
                                            TIME_TO_SEC(
                                                time(t1.accident_time)
                                                ) / 3600
                                            ) * 3600
                                        )
                                )
                            )  as `approx_accident_datetime`,
                        t1.longitude,
                        t1.latitude
                    FROM {table_name} t1
                        JOIN {table_name_to_join} t2 ON t1.day_id = t2.day_id
                        WHERE YEAR(t2.accident_date) = :target_year
                        GROUP BY t1.longitude, t1.latitude, t1.accident_id,
                                 t2.accident_date, t1.accident_time;
            """

    # 3. 從MySQL server取得資料表
    logger.info(f"Querying TABLE {table_name} FROM DATABASE {database}...")
    df_acc = get_table_from_sqlserver(
        query, {"target_year": target_year}, database=database
    )
    logger.info(
        f"Finished the query! The fetched result contains columns: \n {df_acc.columns}"
    )
    # df_acc: ['accident_id', 'approx_accident_datetime', 'longitude', 'latitude']

    # 4. 經緯度進位到氣象網格。必須與 e_get_uniq_acc_geo 用同一支函式，
    # 否則 t_fact_hourly_weather 的 merge 會一列都對不上（ADR-0012）。
    df_acc["lat_round"] = round_to_weather_grid(df_acc["latitude"])
    df_acc["lon_round"] = round_to_weather_grid(df_acc["longitude"])

    df_acc = df_acc.loc[
        :, ["accident_id", "lat_round", "lon_round", "approx_accident_datetime"]
    ]

    # 5. 轉換成str，與天氣側的 datetime_ISO8601 對得上
    df_acc["approx_accident_datetime"] = df_acc["approx_accident_datetime"].astype(str)
    logger.info(
        f"FOR Year {target_year}: \nGot {len(df_acc)} locations "
        f"from the TABLE {table_name} containing {len(df_acc)} accidents."
    )

    return df_acc


"""========================定義TASK 2========================"""


def prep_batch_plan(
    df_acc_unique_loc: pd.DataFrame, target_year: int, batch_size=50
) -> list[dict]:
    """盤點還缺哪些（觀測點, 月），切成批次後把各批的經緯度清單存到 GCS 的 tmp 路徑。

    逐月比對「該有哪些觀測點檔案」與「GCS 上實際有哪些」，缺的才排進計畫。
    已抓完整的月（見 `is_month_complete()`）排除既有檔案；尚未抓完整的月則
    整月重抓覆蓋 —— OpenMeteo 沒有 append，要多幾天就得重寫整個月的檔案。

    回傳空 list 是**正常結果**，代表該年度已全部抓完，不是故障。

    :param df_acc_unique_loc: 經過進位與去重而得到的事故地經緯度
    :type df_acc_unique_loc: pd.DataFrame
    :param target_year: 要從GCS查詢哪一年份的天氣觀測資料
    :type target_year: int
    :param batch_size: 幾個觀測點為一批次，預設值為50
    :return: 每個元素是一批的參數，供 Airflow 的 `expand_kwargs()` 展開
    :rtype: list[dict]
    """
    today = datetime.now(TAIPEI).date()

    # 1. 每個觀測點對應的檔名。與 e_crawler_weatherapi() 存檔時走同一支函式，
    #    兩邊才不會像 ADR-0013 之前那樣組出對不上的名字。
    df_acc_uniq_loc = df_acc_unique_loc.copy()
    df_acc_uniq_loc["file_name"] = [
        blob_file_name(lat, lon)
        for lat, lon in zip(
            df_acc_uniq_loc["lat_round"].tolist(),
            df_acc_uniq_loc["lon_round"].tolist(),
        )
    ]

    # 2. 逐月盤點缺口，並把每一批的 DataFrame 存到 GCS 的 tmp 路徑
    months = months_to_fetch(target_year, today)
    batch_plan: list[dict] = []
    existing_total = 0

    for month in months:
        prefix = weather_data_prefix(target_year, month)
        existing = {
            Path(f).name
            for f in gcs_utils.list_parquet(bucket=WEATHER_BUCKET, prefix=prefix)
        }
        existing_total += len(existing)

        if is_month_complete(target_year, month, today):
            df_needed = df_acc_uniq_loc[~df_acc_uniq_loc["file_name"].isin(existing)]
        else:
            # 該月尚未抓完整，既有檔案涵蓋的天數不足，整月重抓覆蓋
            df_needed = df_acc_uniq_loc

        for i in range(0, len(df_needed), batch_size):
            batch_id = i // batch_size

            # index=True 是本呼叫點特有的：暫存檔要能還原成與切片當下相同的 DataFrame。
            gcs_utils.write_parquet(
                bucket=WEATHER_BUCKET,
                object_name=_batch_object(target_year, month, batch_id),
                df=df_needed.iloc[i : i + batch_size],
                index=True,
            )

            batch_plan.append(
                {"batch_id": batch_id, "target_year": target_year, "month": month}
            )

    # 3. 進度。DagRun 的成功／失敗看不出抓取有沒有推進（下游 trigger_rule 是
    #    all_done），這一行才是判斷「還在前進」還是「卡住了」的依據（ADR-0013）。
    logger.info(
        f"{target_year} 年共需 {len(months) * len(df_acc_uniq_loc)} 個（觀測點, 月）"
        f"組合（{len(df_acc_uniq_loc)} 個觀測點 × {len(months)} 個月），"
        f"GCS 上已有 {existing_total} 個，本次排出 {len(batch_plan)} 個批次"
    )

    return batch_plan


""" == == == == == == == == == == == ==定義TASK 3 == == == == == == == == == == == =="""


def e_crawler_weatherapi(batch_id: int, target_year: int, month: int) -> str:
    """Extract: 讀取（月, 批號）對應的經緯度清單，向 OpenMeteo 請求該月天氣並落地 GCS。

    請求的單位是「一批觀測點 × 一個月」，額度因此是事先算得出來的常數
    （`批次大小 × 該月天數 / 25`），而不是隨「這批缺幾個月」浮動（ADR-0013 決策四）。

    :param batch_id: 批號
    :type batch_id: int
    :param target_year: 說明向OpenMeteo historical weather API請求哪一年度的天氣觀測資料
    :type target_year: int
    :param month: 要請求哪一個月，1～12
    :type month: int
    :return: GCS bucket 名稱字串
    :rtype: str
    :raises RuntimeError: 回傳筆數與請求地點數不符，或寫入失敗率過高
    """
    logger.info(f"Processing {target_year}-{month:02d} batch no {batch_id}......")
    # 1. 從GCS上打開df_one_batch
    df_one_batch = gcs_utils.read_parquet(
        bucket=WEATHER_BUCKET,
        object_name=_batch_object(target_year, month, batch_id),
    )
    df_one_batch["lat_round"] = round_to_weather_grid(df_one_batch["lat_round"])
    df_one_batch["lon_round"] = round_to_weather_grid(df_one_batch["lon_round"])

    # 2. Calling for API時，需要將經緯度以字串形式傳入參數，故遍歷df_one_batch把經度、緯度分別組合出一組字串
    lat_values = df_one_batch["lat_round"].tolist()
    lon_values = df_one_batch["lon_round"].tolist()
    lat_round_lst = [str(lat_num) for lat_num in lat_values]
    lon_round_lst = [str(lon_num) for lon_num in lon_values]
    lats_str = ",".join(lat_round_lst)
    lons_str = ",".join(lon_round_lst)

    # 3. 日期區間就是這個月，迄日不超過 API 查得到的最新日期
    start_date, end_date = month_date_range(
        target_year, month, datetime.now(TAIPEI).date()
    )

    # 4. Calling for API
    records = _request_weather_api(
        lats_str, lons_str, start_date, end_date, WEATHER_VARIABLES
    )

    # 5. 回傳筆數必須與請求的地點數相同 —— 下方是**按位置**把天氣掛回事故座標，
    # 一旦筆數對不上，位置對應就不再成立，天氣會被掛到錯誤的地點。
    # 這種錯位不會自己報錯，只會靜靜寫進 MySQL（同 ADR-0010 的教訓），故先斷言。
    expected = len(lat_round_lst)
    if len(records) != expected:
        raise RuntimeError(
            f"{target_year}-{month:02d} 批次 {batch_id} 請求 {expected} 個地點"
            f"但回傳 {len(records)} 筆，"
            f"位置對應不再成立，拒絕寫入以免天氣掛到錯誤的座標"
        )

    # 6. 將請求結果做簡易清洗後就立即存成parquet，備份資料源。
    saved = 0
    for j, record in enumerate(records):
        if "hourly" not in record:
            # 分母是「請求的地點數」，因此這裡略過即等同計為失敗。
            # 改動前是相反的：不含 hourly 就整段跳過，既不計入失敗率也不留任何紀錄，
            # 於是 [{}] 這類回應會得到 0/1 == 0、被算成完全成功 ——
            # 一筆都沒存到卻顯示成功，且事後從 log 完全無從察覺。
            logger.warning(
                f"{target_year}-{month:02d} 批次 {batch_id} "
                f"第 {j + 1} 個地點的回應不含 hourly，略過"
            )
            continue

        df_a_loc_hourly = pd.DataFrame(record["hourly"])

        # 補回lat_round、lon_round，以便後續追溯這筆天氣資料是靠近哪一個事故地經緯度。
        # 刻意不用回應中的 latitude/longitude —— 那是網格中心座標，與事故端的值不同，
        # 用它會讓下游 t_fact_hourly_weather 的 merge 全部落空。
        df_a_loc_hourly["latitude_round"] = lat_values[j]
        df_a_loc_hourly["longitude_round"] = lon_values[j]

        # 置換成想要的欄位名稱
        # 但盡可能的跟response中欄位名稱一樣，此步驟能做到與API端口隔離就好。
        df_a_loc_hourly.columns = [
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

        # 定義存檔名稱。與 prep_batch_plan() 盤點時走同一支函式，兩邊才對得上。
        file_name = (
            f"{weather_data_prefix(target_year, month)}/"
            f"{blob_file_name(lat_values[j], lon_values[j])}"
        )

        # 存成 Parquet 較節省空間(直接存到GCS上)
        try:
            logger.info(f"Saving the {(j + 1)}/{expected} file....")
            gcs_utils.write_parquet(
                bucket=WEATHER_BUCKET,
                object_name=file_name,
                df=df_a_loc_hourly,
            )
        except Exception as exc:
            # 單一檔案失敗仍可能由整批重試自癒，故記 warning 而非 error（ADR-0006）
            logger.warning(f"寫入 {file_name} 失敗：{exc}")
        else:
            saved += 1
            logger.info(f"Successfully saved the file, {file_name}!")

    # 7. 結算失敗率。分母是請求的地點數，因此「缺 hourly」「寫入失敗」
    # 「回傳筆數不足」三種靜默損失都算得進來。
    label = f"{target_year}-{month:02d} 批次 {batch_id}"
    failure_rate = 1 - saved / expected
    if failure_rate > MAX_FAILURE_RATE:
        raise RuntimeError(f"{label} 寫入失敗率過高：成功 {saved}/{expected}")
    if failure_rate > 0:
        logger.warning(f"{label} 部分失敗：成功 {saved}/{expected}")

    logger.info(f"Finished {label}! 成功 {saved}/{expected}")

    # 8. sleep緩解server負擔
    time.sleep(random.uniform(5, 10))
    return WEATHER_BUCKET
