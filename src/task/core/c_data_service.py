"""業務運算層：夜市周邊事故計算、快取讀寫與全臺總表聚合。

介於查詢層（`c_db`）與前端頁面之間，部分聚合計算函式會透過 DAG 自動化執行。
函式分兩種快取策略：

- 讀取型（前端直接呼叫）走 read-through：先讀 Redis，沒有就查 MySQL 再補寫快取。
  快取寫入失敗只記 warning，不影響本次回傳，因為資料已經拿到了。
- 預計算型（DAG 呼叫）：負責定期做運算資源吃比較重的預計算，產出快取寫入 Redis，寫入失敗即拋出。


Notes:
    查詢粒度與快取失敗的處理分別參考 ADR-0009 與 ADR-0003。
"""

import itertools
import uuid
from datetime import datetime

import numpy as np
import pandas as pd
from redis.exceptions import RedisError

from src.task.core.c_db import (
    get_accident_hotspots,
    get_accident_table_pedestrian_involved_in,
    get_night_markets_table,
)
from src.util.logger_crtx import get_logger
from src.util.redis_utils import delete_cache, get_cache, set_cache

logger = get_logger(__name__)


# ========================== 由前端調用 ==========================
# 由app、act1、act2調用
def get_all_nightmarkets() -> pd.DataFrame:
    """讀取全臺夜市主檔，清洗座標並修正離島的地區歸屬。

    先讀 Redis 快取（鍵為 `market:list_all_auto_v3`，且必須含 `area_road` 與
    `region` 兩欄才算命中），沒有才查 MySQL。查回來後把六個座標欄轉成數值、
    把名稱或地址含琉球、蘭嶼、綠島的夜市歸到「東部與東部離島」，再剔除座標
    缺漏的列。取得資料後會補寫快取（存活 12 小時），寫入失敗只記 warning。

    Returns:
        pandas.DataFrame: 夜市主檔，形如：

            nightmarket_name  region  city    district  latitude   longitude   googlemap_rating
            士林夜市          北部    臺北市  士林區    25.088100  121.524300  4.2
            小琉球夜市        東部與東部離島  屏東縣  琉球鄉  22.342100  120.371500  4.0

    Raises:
        RedisError: 讀取快取失敗。
        SQLAlchemyError: 查詢 MySQL 失敗。
        KeyError: 查回的資料缺少座標或名稱欄位。
    """
    # 先拿cache_key從Redis取資料
    cache_key = "market:list_all_auto_v3"
    cached = get_cache(cache_key)
    if cached is not None:
        # 快取存的是 to_dict("records") 的 list[dict]，讀回來要轉回 DataFrame
        df_cached = pd.DataFrame(cached)
        if "area_road" in df_cached.columns and "region" in df_cached.columns:
            return df_cached

    # 如果回傳 None 就改讀 MySQL 資料庫，並且補存入 Redis 為下一次讀取加速
    try:
        logger.info("Redis快取層無資料！改讀MySQL......")
        df = get_night_markets_table()
        # 資料清洗：確保經緯度為數值型別，並補上四層級分類標籤供前端下拉選單使用
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["northeast_latitude"] = pd.to_numeric(
            df["northeast_latitude"], errors="coerce"
        )
        df["northeast_longitude"] = pd.to_numeric(
            df["northeast_longitude"], errors="coerce"
        )
        df["southwest_latitude"] = pd.to_numeric(
            df["southwest_latitude"], errors="coerce"
        )
        df["southwest_longitude"] = pd.to_numeric(
            df["southwest_longitude"], errors="coerce"
        )

        # 處理附屬離島特例強制劃分
        df.loc[df["area_road"].str.contains("琉球|蘭嶼|綠島", na=False), "region"] = (
            "東部與東部離島"
        )
        df.loc[
            df["nightmarket_name"].str.contains("琉球|蘭嶼|綠島", na=False), "region"
        ] = "東部與東部離島"

        # 剔除經緯度遺漏的髒資料 (正常來說不會有)
        df_all_nm = df.dropna(subset=["latitude", "longitude"], how="any")
    except Exception:
        # 本層只是轉手，僅記錄發生什麼；traceback 由邊界層帶 exc_info 輸出
        logger.error("夜市事實表從 MySQL 讀取失敗")
        raise

    # read-through 快取：資料已取得，寫入失敗不影響本次回傳
    try:
        result = df_all_nm.to_dict("records")
        set_cache(cache_key, result, ttl=43200)
    except RedisError:
        # 本層即為例外停止傳播之處，故完整記錄
        logger.warning(f"快取寫入失敗，不影響本次回傳: {cache_key}", exc_info=True)

    # 無論Redis寫入是否成功，只要MySQL有拿到資料就回傳，確保客戶可以優先取得資料
    return df_all_nm


# 由 act1 的夜市下拉選單調用
def get_nightmarkets_for_page_selector() -> pd.DataFrame:
    """在夜市主檔上補一組下拉選單用的別名欄位。

    只做欄位改名，不另外讀 MySQL 也不另開快取；主檔的讀取、清洗與離島歸屬
    一律由 `get_all_nightmarkets()` 負責，避免兩份邏輯各自漂移。行政區取
    `district`（xx 區），不是街道地址的 `area_road`。

    Returns:
        pandas.DataFrame: 原主檔再加上別名欄位，形如：

            nightmarket_name  MarketName  Region  City    AdminDistrict  lat        lon
            士林夜市          士林夜市    北部    臺北市  士林區         25.088100  121.524300

            主檔為空時原樣回傳空 DataFrame。

    Raises:
        RedisError: 讀取快取失敗。
        SQLAlchemyError: 查詢 MySQL 失敗。
    """
    df = get_all_nightmarkets().copy()
    if df.empty:
        return df

    df["lat"] = df["latitude"]
    df["lon"] = df["longitude"]
    df["MarketName"] = df["nightmarket_name"]
    df["Region"] = df["region"]
    df["City"] = df["city"]
    # 行政區取 district（xx區）。area_road 是街道地址，不是行政區
    df["AdminDistrict"] = df["district"]
    return df


# 由 act1 調用
def haversine_distance(
    lat1: float | int, lon1: float | int, lat2: float | int, lon2: float | int
) -> float:
    """以半正矢公式計算地球表面兩點之間的大圓距離，單位公里。

    以 numpy 運算，因此四個參數都可以傳入 Series 或陣列做向量化計算，
    比逐列迴圈快上許多。地球半徑取 6371 公里。

    Args:
        lat1 (float | int): 第一點的緯度。
        lon1 (float | int): 第一點的經度。
        lat2 (float | int): 第二點的緯度。
        lon2 (float | int): 第二點的經度。

    Returns:
        float: 兩點間的距離（公里）；傳入陣列時回傳同長度的陣列。
    """
    R = 6371
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


# 由 app 調用
def get_accident_heatmap_data(sample_size: int = 8000):
    """統計熱力圖用的事故熱點資料。

    先讀 Redis 快取（鍵為 `traffic:global_heatmap_lite_v2`），沒有才查 MySQL。
    查詢回來的已經是聚合過的熱點 —— 落在臺灣範圍外的座標與重複不足 3 次的座標
    都已排除。熱點數仍超過 `sample_size` 時隨機抽樣（固定亂數種子，結果可重現），
    避免前端地圖卡頓。取得後補寫快取（存活 12 小時）。

    Args:
        sample_size (int): 熱點數上限，超過就抽樣，預設 8000。

    Returns:
        pandas.DataFrame: 熱點資料，形如：

            latitude   longitude   count
            25.088100  121.524300  17
            24.152600  120.658700  9

            查無熱點時為空 DataFrame。

    Raises:
        RedisError: 讀取快取失敗。
        SQLAlchemyError: 查詢 MySQL 失敗。

    Notes:
        減量由 SQL 完成而非 pandas，參考 ADR-0016。
    """
    # 先拿 cache_key 從 Redis 取資料
    cache_key = "traffic:global_heatmap_lite_v2"
    cached = get_cache(cache_key)
    if cached:
        df_cached = pd.DataFrame(cached)
        return df_cached

    # 如果回傳 None 就改讀 MySQL 資料庫，並且補存入 Redis 為下一次讀取加速
    try:
        # 台灣國土範圍與「同一座標重複 3 次以上才算熱點」都交給 SQL，
        # 回來的列數是熱點數而非事故數
        df = get_accident_hotspots(
            lat_min=21.755,
            lat_max=25.93916,
            lon_min=119.30083,
            lon_max=124.56916,
            min_count=3,
        )
        if df.empty:
            return df

        df["latitude"] = df["latitude"].astype("float64")
        df["longitude"] = df["longitude"].astype("float64")

        # 如果熱力圖資料點仍超過預設8000點 (sample_size)，強制隨機抽樣，避免地圖卡頓
        if len(df) > sample_size:
            df = df.sample(n=sample_size, random_state=42)

    except Exception:
        # 本層只是轉手，僅記錄發生什麼；traceback 由邊界層帶 exc_info 輸出
        logger.error("熱點圖資料從 MySQL 讀取失敗")
        raise
    # read-through 快取：資料已取得，寫入失敗不影響本次回傳
    try:
        result = df.to_dict("records")
        set_cache(cache_key, result, ttl=43200)
    except RedisError:
        # 本層即為例外停止傳播之處，故完整記錄
        logger.warning(f"快取寫入失敗，不影響本次回傳: {cache_key}", exc_info=True)

    # 無論Redis寫入是否成功，只要MySQL有拿到資料就回傳，確保客戶可以優先取得資料
    return df


# 由 act3 調用，現已無 act3
def get_pedestrian_stats_by_region_monthly():
    """依地區與年月統計行人涉入的事故件數。

    先讀 Redis 快取（鍵為 `analysis:pedestrian_region_month`），沒有才查 mart 層
    分析表，取得後補寫快取（存活 12 小時），寫入失敗只記 warning。

    目前沒有呼叫端，預留給尚未建立的 act3 分頁。

    Returns:
        pandas.DataFrame: 統計結果，依年月與地區排序，形如：

            accident_yearmonth  region  counts
            2024-01             北部    412
            2024-01             中部    233

    Raises:
        RedisError: 讀取快取失敗。
        SQLAlchemyError: 查詢 MySQL 失敗。
    """
    # 先拿cache_key從Redis取資料
    cache_key = "analysis:pedestrian_region_month"
    cached = get_cache(cache_key)
    if cached is not None:
        df_cached = pd.DataFrame(cached)
        return df_cached

    # 如果回傳None就改讀MySQL資料庫，並且補存入Redis為下一次讀取加速
    try:
        logger.info("Redis快取層無資料！改讀MySQL......")
        query = """SELECT accident_yearmonth,
                          region,
                          COUNT(distinct accident_id) AS `counts`
                        FROM analysis_pesdestrian_involving_accident
                            GROUP BY accident_yearmonth, region
                            ORDER BY accident_yearmonth, region;"""
        df = get_accident_table_pedestrian_involved_in(query)
    except Exception:
        # 本層只是轉手，僅記錄發生什麼；traceback 由邊界層帶 exc_info 輸出
        logger.error("Table analysis_pesdestrian_involving_accident 從 MySQL 查詢失敗")
        raise
    try:
        result = df.to_dict("records")
        set_cache(cache_key, result, ttl=43200)
    except RedisError:
        logger.warning(f"快取寫入失敗，不影響本次回傳: {cache_key}", exc_info=True)

    # 無論Redis寫入是否成功，只要MySQL有拿到資料就回傳，確保客戶可以優先取得資料
    return df


# 由 act3 調用，現已無 act3
def get_pedestrian_trend(lat=None, lon=None, radius_km=0.5):
    """統計行人事故的逐月趨勢，給定座標時只計算該範圍內。

    不給座標時統計全臺，快取鍵是固定的；給了座標則以該點為中心、依半徑換算出
    方框範圍過濾（1 度約 111 公里），快取鍵帶上座標。座標一律走 bind parameter
    傳入查詢。先讀快取，沒有才查 mart 層分析表並補寫（存活 12 小時）。

    目前沒有呼叫端，預留給尚未建立的 act3 分頁。

    Args:
        lat (float | None): 中心點緯度；與 `lon` 任一為 `None` 就統計全臺。
        lon (float | None): 中心點經度。
        radius_km (float): 方框半徑，單位公里，預設 0.5。

    Returns:
        pandas.DataFrame: 逐月統計，依年月排序，形如：

            accident_yearmonth  counts
            2024-01             1245
            2024-02             1103

    Raises:
        RedisError: 讀取快取失敗。
        SQLAlchemyError: 查詢 MySQL 失敗。
    """
    # 拼湊cache_key

    # 如不指定經緯度範圍
    if lat is None or lon is None:
        cache_key = "analysis:pedestrian_trend_global_v2"
        where_clause = ""
        params = {}
    else:
        # 如有指定經緯度範圍
        cache_key = (
            f"analysis:pedestrian_trend_local_v2:{round(lat, 4)}_{round(lon, 4)}"
        )
        offset = float(radius_km) / 111.0  # 1度約111.0 km，將圓半徑轉換成經緯度

        # 範圍邊界
        where_clause = """
            WHERE latitude BETWEEN :min_lat AND :max_lat
              AND longitude BETWEEN :min_lon AND :max_lon
        """
        params = {
            "min_lat": lat - offset,
            "max_lat": lat + offset,
            "min_lon": lon - offset,
            "max_lon": lon + offset,
        }

    # 拿cache_key從Redis取看看資料
    cached = get_cache(cache_key)
    if cached is not None:
        df_cached = pd.DataFrame(cached)
        return df_cached

    # 如果回傳None就改讀MySQL資料庫，並且補存入Redis為下一次讀取加速
    try:
        logger.info("Redis快取層無資料！改讀MySQL......")
        query = f"""SELECT accident_yearmonth,
                          COUNT(distinct accident_id) AS `counts`
                        FROM analysis_pesdestrian_involving_accident
                            {where_clause}
                            GROUP BY accident_yearmonth
                            ORDER BY accident_yearmonth;"""
        # where_clause 內含 :min_lat 等 bind parameter，params 必須一併傳入
        df = get_accident_table_pedestrian_involved_in(query, params)
    except Exception:
        # 本層只是轉手，僅記錄發生什麼；traceback 由邊界層帶 exc_info 輸出
        logger.error("Table analysis_pesdestrian_involving_accident 從 MySQL 查詢失敗")
        raise
    # read-through 快取：資料已取得，寫入失敗不影響本次回傳
    try:
        result = df.to_dict("records")
        set_cache(cache_key, result, ttl=43200)
    except RedisError:
        # 本層即為例外停止傳播之處，故完整記錄
        logger.warning(f"快取寫入失敗，不影響本次回傳: {cache_key}", exc_info=True)

    # 無論Redis寫入是否成功，只要MySQL有拿到資料就回傳，確保客戶可以優先取得資料
    return df


# ========================== 由 DAG 調用 ==========================

# 粗篩框的半徑，同時是 aggregate_national_master() 讀取 key 所帶的半徑
ROUGH_RADIUS_KM = 3.0

# 前端與聚合階段實際會用到的欄位；查詢仍撈全表欄位，缺欄位時在此靜默跳過
ACCIDENT_MAP_COLUMNS = [
    "accident_id",
    "accident_date",
    "accident_year",
    "accident_hourtime",
    "accident_time",
    "accident_weekday",
    "cause_analysis_major_individual_grouped",
    "party_action_major",
    "weather_condition",
    "light_condition",
    "road_surface_condition",
    "latitude",
    "longitude",
    "death_count",
    "injury_count",
    "accident_type_major_grouped",
    "cause_analysis_minor_individual",
]


def _build_batch_bbox_query(
    batch: list[dict], radius_km: float = ROUGH_RADIUS_KM
) -> tuple[str, dict]:
    """組出涵蓋整批夜市粗篩方框聯集的單一查詢與其綁定參數。

    查詢的粒度與使用它的粒度一致：呼叫端服務的是一個批次，查詢就是一句，
    各夜市的方框條件以 OR 串起來。逐夜市各發一次的話，同一都會區的鄰近夜市
    會把重疊區域的事故重複撈回來，而且被查的分析表沒有索引，每一次都是一趟
    全表掃描。座標一律走 bind parameter，不以字串內插回查詢。

    Args:
        batch (list[dict]): 一個批次的夜市，每筆須含 `lat` 與 `lon`。
        radius_km (float): 粗篩方框的半徑，單位公里，預設 3.0。

    Returns:
        tuple[str, dict]: 帶具名佔位符的查詢字串與對應的參數字典，形如：

            (
                "SELECT * FROM analysis_pesdestrian_involving_accident WHERE "
                "(latitude BETWEEN :min_lat_0 AND :max_lat_0 "
                "AND longitude BETWEEN :min_lon_0 AND :max_lon_0)",
                {
                    "min_lat_0": 25.061081,
                    "max_lat_0": 25.115118,
                    "min_lon_0": 121.497273,
                    "max_lon_0": 121.551327,
                },
            )

    Raises:
        KeyError: 批次中某筆夜市缺少 `lat` 或 `lon`。

    Notes:
        參考 ADR-0009。
    """
    offset = radius_km / 111  # 1度大約等於111公里
    clauses = []
    params: dict[str, float] = {}

    for i, a_nightmarket in enumerate(batch):
        nm_lat, nm_lon = a_nightmarket["lat"], a_nightmarket["lon"]
        params[f"min_lat_{i}"] = nm_lat - offset
        params[f"max_lat_{i}"] = nm_lat + offset
        params[f"min_lon_{i}"] = nm_lon - offset
        params[f"max_lon_{i}"] = nm_lon + offset
        clauses.append(
            f"(latitude BETWEEN :min_lat_{i} AND :max_lat_{i}"
            f" AND longitude BETWEEN :min_lon_{i} AND :max_lon_{i})"
        )

    query = (
        "SELECT * FROM analysis_pesdestrian_involving_accident WHERE "
        + " OR ".join(clauses)
    )
    return query, params


# 由 dags/d06_precompute_to_redis 調用
def get_and_slice_nightmarkets_multibatches() -> list:
    """把夜市清單補齊邊界座標後切成批次存入 Redis，回傳各批的鍵。

    每 30 個夜市一批。夜市若沒有東北、西南兩個邊界端點，就以中心點往該方向
    擴 0.005 度（約 500 公尺）補上。批次資料本身放進 Redis（存活 12 小時），
    只回傳輕量的鍵字串，避免 DAG 的 XCom 塞進大量資料。

    Returns:
        list[str]: 各批次在 Redis 中的鍵，形如：

            [
                "xcom_claim_check:3f2a8c1d-...:batch_0",
                "xcom_claim_check:3f2a8c1d-...:batch_1",
            ]

    Raises:
        RedisError: 讀取或寫入快取失敗。
        SQLAlchemyError: 查詢 MySQL 失敗。
    """
    # 1. 剔除經緯度遺漏 與 經緯度重複
    df_all_nm = get_all_nightmarkets().copy()
    df_all_nm = df_all_nm.dropna(subset=["latitude", "longitude"], how="any")
    df_all_nm = df_all_nm.drop_duplicates(subset=["latitude", "longitude"])

    # 2. 重構前這裡是用for-loop in iterrows()，這次改成用向量化處理
    # 先確保中心點是數值
    df_all_nm["latitude"] = pd.to_numeric(df_all_nm["latitude"], errors="coerce")
    df_all_nm["longitude"] = pd.to_numeric(df_all_nm["longitude"], errors="coerce")

    # 使用 fillna 處理東北與西南角邊界，若為空值，則預設以夜市中心點往該方向擴展500 m (0.005度)
    n_lat = pd.to_numeric(df_all_nm["northeast_latitude"], errors="coerce").fillna(
        df_all_nm["latitude"] + 0.005
    )
    s_lat = pd.to_numeric(df_all_nm["southwest_latitude"], errors="coerce").fillna(
        df_all_nm["latitude"] - 0.005
    )
    e_lon = pd.to_numeric(df_all_nm["northeast_longitude"], errors="coerce").fillna(
        df_all_nm["longitude"] + 0.005
    )
    w_lon = pd.to_numeric(df_all_nm["southwest_longitude"], errors="coerce").fillna(
        df_all_nm["longitude"] - 0.005
    )

    # 建立目標結構的 DataFrame
    df_all_valid_nm = pd.DataFrame(
        {
            "name": df_all_nm["nightmarket_name"].astype(str),
            "city": df_all_nm["city"].astype(str),
            "rating": pd.to_numeric(
                df_all_nm.get("googlemap_rating", 0.0), errors="coerce"
            )
            .fillna(0.0)
            .astype("float64"),
            "lat": df_all_nm["latitude"].astype("float64"),
            "lon": df_all_nm["longitude"].astype("float64"),
            "n_lat": n_lat.astype("float64"),
            "s_lat": s_lat.astype("float64"),
            "e_lon": e_lon.astype("float64"),
            "w_lon": w_lon.astype("float64"),
        }
    )

    # 一次性轉成list of dicts
    valid_markets = df_all_valid_nm.to_dict("records")

    # 3. 將全台夜市切成10個批次，預估每個批次30個夜市
    batch_size = 30
    batches = [
        valid_markets[i : i + batch_size]
        for i in range(0, len(valid_markets), batch_size)
    ]
    # batches: a list containing many "list of 30 dict"

    # 4. 將資料存入 Redis，只產生極輕量的 String 號碼牌
    batch_uuid = str(uuid.uuid4())
    keys_of_batches = []

    for i, batch in enumerate(batches):
        key_of_a_batch = f"xcom_claim_check:{batch_uuid}:batch_{i}"
        set_cache(key_of_a_batch, batch, 43200)  # 存入Redis，保留12小時
        keys_of_batches.append(key_of_a_batch)  # 只回傳key給其他函式用

    logger.info(f"已生成 {len(keys_of_batches)} 個Redis keys。")
    return keys_of_batches  # 回傳的只會是 ['xcom_claim_check:...', ...] 這樣短字串陣列，避免 XCom 爆表問題


# 由 dags/d06_precompute_to_redis 調用
def cal_accidents_nearby_nightmarket(
    batch_key: str,
    radius_m_list: list[float | int] | None = None,
    year_targets: list[int | str] | None = None,
):  # process_market_batch()原名
    """計算一個批次內每個夜市周邊的事故，把粗篩與細篩結果寫入 Redis。

    整批只發一次查詢，撈回這批夜市 3 公里方框的聯集，再於記憶體中逐夜市切片，
    因此查詢失敗即整批失敗，沒有部分成功可言。每個夜市先寫一份 3 公里的粗篩
    結果，其鍵 `traffic:nearby_v12:{緯度}_{經度}_3.0_all_sample` 是
    `aggregate_national_master()` 唯一會讀的，因此無條件寫入；接著依半徑與年份
    的組合寫入細篩結果，與粗篩同鍵的那一圈會跳過。

    寫入失敗即 raise。

    Args:
        batch_key (str): 批次夜市清單在 Redis 中的鍵，
            即 `get_and_slice_nightmarkets_multibatches()` 回傳的其中一個。
        radius_m_list (list[float | int] | None): 細篩半徑清單，單位公尺，
            預設 `[3000]`。
        year_targets (list[int | str] | None): 細篩年份清單，`"all_sample"`
            代表不分年份，預設 `["all_sample"]`。

    Returns:
        str: 形如 `"xcom_claim_check:3f2a8c1d-...:batch_0 處理完成"` 的訊息。

    Raises:
        ValueError: 批次在 Redis 中不存在或為空。
        RedisError: 讀取或寫入快取失敗。
        SQLAlchemyError: 查詢 MySQL 失敗。

    Notes:
        整批一次查詢與快取鍵的契約參考 ADR-0009。
    """
    radius_m_list = radius_m_list or [3000]
    year_targets = year_targets or ["all_sample"]

    # 先拿cache_key從Redis取夜市資料。
    batch = get_cache(batch_key)
    if not batch:
        raise ValueError(f"批次 {batch_key} 在 Redis 中不存在或為空，無法計算附近事故")

    # 一個批次一次查詢：撈回這 30 個夜市 3 公里框的聯集
    query, params = _build_batch_bbox_query(batch)
    df_batch = get_accident_table_pedestrian_involved_in(query, params)

    valid_cols = [c for c in ACCIDENT_MAP_COLUMNS if c in df_batch.columns]
    df_batch = df_batch[valid_cols].copy()

    if not df_batch.empty:
        df_batch["latitude"] = pd.to_numeric(df_batch["latitude"], errors="coerce")
        df_batch["longitude"] = pd.to_numeric(df_batch["longitude"], errors="coerce")
        df_batch["accident_year"] = pd.to_numeric(
            df_batch["accident_year"], errors="coerce"
        )

    max_offset = ROUGH_RADIUS_KM / 111  # 1度大約等於111公里。將3公里轉成度

    # 遍歷每個夜市，從批次結果中切出該地附近3公里的方形區域，粗篩。
    for a_nightmarket in batch:  # a_nightmarket: a dict
        nm_lat, nm_lon = a_nightmarket["lat"], a_nightmarket["lon"]

        # reset_index：粗篩結果現在是批次結果的切片，索引須還原成與「逐夜市各查一次」
        # 時代相同的 0..n-1，快取內容才逐列等價
        df_nearby_accidents = df_batch[
            df_batch["latitude"].between(nm_lat - max_offset, nm_lat + max_offset)
            & df_batch["longitude"].between(nm_lon - max_offset, nm_lon + max_offset)
        ].reset_index(drop=True)

        # 這把 key 是 aggregate_national_master() 唯一會讀的，因此無條件寫入，
        # 不能取決於呼叫端有沒有把 3000 與 "all_sample" 傳進 radius_m_list／year_targets
        cache_key_rough = f"traffic:nearby_v12:{nm_lat:.4f}_{nm_lon:.4f}_{ROUGH_RADIUS_KM:.1f}_all_sample"
        set_cache(cache_key_rough, df_nearby_accidents, 43200)
        logger.info(f"cache_key_rough: {cache_key_rough} 存取成功")

        # 細篩：半徑清單與年份清單，的組合、來計算車禍與夜市的交集
        for r_m, y_target in itertools.product(radius_m_list, year_targets):
            r_km = float(r_m) / 1000.0
            cache_key_per_product = (
                f"traffic:nearby_v12:{nm_lat:.4f}_{nm_lon:.4f}_{r_km:.1f}_{y_target}"
            )
            # 預設參數下這一圈與粗篩是同一把 key、同一份資料，寫第二次沒有意義
            if cache_key_per_product == cache_key_rough:
                continue

            offset = r_km / 111  # 1度大約等於111公里。將公里轉成度

            # 製作過濾條件
            mask = df_nearby_accidents["latitude"].between(
                nm_lat - offset, nm_lat + offset
            ) & df_nearby_accidents["longitude"].between(
                nm_lon - offset, nm_lon + offset
            )
            if y_target != "all_sample":
                mask &= df_nearby_accidents["accident_year"] == int(y_target)

            # 跳用條件完成細篩並存入新的dataframe容器
            df_target = df_nearby_accidents[mask]

            # 預計算路徑：寫入失敗即 raise
            set_cache(cache_key_per_product, df_target, 43200)

    return f"{batch_key} 處理完成"


# 由 dags/d06_precompute_to_redis 調用
def aggregate_national_master(batch_keys: list[dict]):
    """聚合各批次的預計算結果成全臺總表，並產出儀表板用的統計快取。

    先用批次鍵還原全臺夜市清單，逐夜市讀取 `cal_accidents_nearby_nightmarket()`
    寫下的 3 公里粗篩快取，把落在夜市邊界方框內的事故留下並貼上夜市名稱、
    縣市與評分。接著補上年、季、月、星期、小時等時間欄位，算出 PDI 分數
    （死亡數乘 10 加受傷數乘 2，17 點後與 0 點的事故再乘 1.5）。

    產出三種快取：全臺總表 `market:national_master_df`、巨觀統計
    `traffic:stats:audit_macro`，以及每個夜市各自的
    `traffic:stats:audit_market:{夜市名稱}`，讓前端點擊地圖時能直接取用。
    完成後刪掉批次鍵，釋放 Redis 空間。

    聚合不出任何資料時 raise。

    Args:
        batch_keys (list[str]): 各批次夜市清單在 Redis 中的鍵。

    Raises:
        RuntimeError: 所有批次皆找不到周邊事故快取，聚合不出全臺總表。
        RedisError: 讀取或寫入快取失敗。
        KeyError: 夜市資料缺少邊界座標或名稱等欄位。

    Notes:
        不靜默結束參考 ADR-0003。
    """
    # 先拿cache_key從Redis取所有批次的夜市資料，攤成一個大列表
    all_night_markets = []
    for nm_key in batch_keys:
        data = get_cache(nm_key)  # a list of dicts
        if data:
            all_night_markets.extend(data)  # a list of dicts

    # 讀取剛剛用cal_accidents_nearby_nightmarket()存入 Redis的夜市周遭車禍資訊，
    # 透過 concat聚合成全台大表
    all_dfs = []
    for nm in all_night_markets:
        nm_lat, nm_lon = nm["lat"], nm["lon"]

        # 拼出key
        key = f"traffic:nearby_v12:{nm_lat:.4f}_{nm_lon:.4f}_3.0_all_sample"
        data = get_cache(key)

        if data is not None:
            df = pd.DataFrame(data)
            if not df.empty:
                # 從3 km周遭內再保留，有落在 "夜市範圍內"+"夜市周圍500 m之方框內"的事故
                mask = (df["latitude"].between(nm["s_lat"], nm["n_lat"])) & (
                    df["longitude"].between(nm["w_lon"], nm["e_lon"])
                )
                df_strict = df[mask].copy()

                if not df_strict.empty:
                    # 補上夜市資訊標籤
                    df_strict["nightmarket_name"] = nm["name"]
                    df_strict["nightmarket_city"] = str(nm["city"])
                    df_strict["googlemap_rating"] = float(nm["rating"])
                    all_dfs.append(df_strict)

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)

        # 確保時間特徵完整
        if "accident_date" in final_df.columns:
            final_df["accident_date"] = pd.to_datetime(final_df["accident_date"])

            final_df["Quarter"] = final_df["accident_date"].dt.quarter
            final_df["Month"] = final_df["accident_date"].dt.month
        if "accident_year" in final_df.columns:
            final_df["Year"] = final_df["accident_year"]
        if "accident_weekday" in final_df.columns:
            final_df["Weekday"] = final_df["accident_weekday"]
        if "accident_hourtime" in final_df.columns:
            final_df["Hour"] = final_df["accident_hourtime"]

        # 計算PDI分數
        final_df["weight"] = np.where(
            (final_df["Hour"] >= 17) | (final_df["Hour"] == 0), 1.5, 1.0
        )
        final_df["severity"] = (
            final_df["death_count"] * 10 + final_df["injury_count"] * 2
        )
        final_df["pdi_score"] = final_df["severity"] * final_df["weight"]

        # 存入給其他圖表用的原始巨型 DataFrame
        set_cache("market:national_master_df", final_df, ttl=43200)
        logger.info(
            f"全台夜市周邊總表聚合完成，共 {len(final_df)} 筆精準事故，已存入 Redis。"
        )

        # 新增前端需要的「白天/夜間」時段標籤 (06-18為白天)
        final_df["time_slot"] = final_df["Hour"].apply(
            lambda x: "Day" if 6 <= x < 18 else "Night"
        )

        # 定義共用的聚合函數 (只算總量與 PDI 總和)
        def generate_stats(df_target, groupby_cols):
            """依指定欄位分組，統計事故數與 PDI 總分。

            Args:
                df_target (pandas.DataFrame): 要統計的資料，須含 `accident_id`
                    與 `pdi_score`。
                groupby_cols (list[str]): 分組欄位。

            Returns:
                list[dict]: 每個字典是一組統計，形如：

                    [
                        {
                            "Year": 2024,
                            "Quarter": 1,
                            "Month": 1,
                            "time_slot": "Night",
                            "acc_count": 128,
                            "pdi_total": 642.0,
                        },
                    ]
            """
            res = (
                df_target.groupby(groupby_cols)
                .agg(
                    acc_count=("accident_id", "count"),  # 計算總事故數
                    pdi_total=("pdi_score", "sum"),  # 加總剛算好的 PDI
                )
                .reset_index()
            )
            return res.to_dict("records")

        # 全台夜市周邊總計、各縣市夜市周邊總計
        taiwan_market_stats = generate_stats(
            final_df, ["Year", "Quarter", "Month", "time_slot"]
        )
        city_market_stats = generate_stats(
            final_df, ["nightmarket_city", "Year", "Quarter", "Month", "time_slot"]
        )

        macro_bundle = {
            "taiwan_markets_total": taiwan_market_stats,
            "city_markets_total": city_market_stats,
            "updated_at": str(datetime.now()),
        }

        # 將巨觀數字打包存入一個專屬的Key
        set_cache("traffic:stats:audit_macro", macro_bundle)

        # 微觀統計 (Micro)：單一特定夜市 500m 總計
        # 將 300 個夜市分開存成各自的 Key，讓前端地圖點擊時可以更快拉資料
        market_groups = final_df.groupby("nightmarket_name")
        for m_name, m_df in market_groups:
            m_stats = generate_stats(m_df, ["Year", "Quarter", "Month", "time_slot"])
            set_cache(f"traffic:stats:audit_market:{m_name}", m_stats)

        logger.info("Audit儀表板輕量化統計運算完成，已存入 Redis。")

        # 任務完成後，清空資料，釋放 Redis 寄物櫃空間
        for key in batch_keys:
            delete_cache(key)
    else:
        # 本函式的產出就是全台總表；聚合不出任何資料代表上游預計算未生效，
        # 不能靜默結束讓 task 顯示成功
        raise RuntimeError(
            f"無法聚合全台總表：{len(batch_keys)} 個批次共 "
            f"{len(all_night_markets)} 個夜市，皆未找到周邊事故快取"
        )
