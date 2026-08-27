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

import numpy as np
import pandas as pd
from redis.exceptions import RedisError

from src.task.core.c_db import (
    get_accident_table_pedestrian_involved_in,
    get_night_markets_table,
)
from src.util.logger_crtx import get_logger
from src.util.redis_utils import delete_cache, get_cache, set_cache

logger = get_logger(__name__)

# 本模組寫進 Redis 的所有快取，其存活時間的預設值。修改此常數時需先考量 d06 的排程
# TTL 不得短於 DAG 排程間隔，且建議要是排程間隔的兩倍，容許下一次 DAG 失敗機會。
# 個別函式要用不同的存活時間時，由呼叫端傳 `cache_ttl` 覆寫，不必改這個預設值。
CACHE_TTL_SECONDS = 864000  # 10 天


# ========================== 前端與 DAG 共用 ==========================
# 由 app、act1、act2 與 dags/d06_precompute_to_redis 調用
def get_all_nightmarkets(cache_ttl: int = CACHE_TTL_SECONDS) -> pd.DataFrame:
    """讀取全臺夜市主檔，清洗座標並修正離島的地區歸屬。

    先讀 Redis 快取（鍵為 `market:night_markets_all`，且必須含 `area_road` 與
    `region` 兩欄才算命中），沒有才查 MySQL。查回來後把六個座標欄轉成數值、
    把名稱或地址含琉球、蘭嶼、綠島的夜市歸到「東部與東部離島」，再剔除座標
    缺漏的列。取得資料後會補寫快取，寫入失敗只記 warning。

    Args:
        cache_ttl (int): 快取存活秒數，預設為 `CACHE_TTL_SECONDS`（10 天）。

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
    cache_key = "market:night_markets_all"
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
        # str.contains(...) 在有缺值的欄位上會產生一個 object dtype 的欄，裡面混著 True、False、
        # NaN，含有 NaN 的不可以拿去作為 Mask，所以要補上 na=False，將 NaN 表示為 False，回傳 bool dtype
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
        set_cache(cache_key, result, ttl=cache_ttl)
    except RedisError:
        # 本層即為例外停止傳播之處，故完整記錄
        logger.warning(f"快取寫入失敗，不影響本次回傳: {cache_key}", exc_info=True)

    # 無論Redis寫入是否成功，只要MySQL有拿到資料就回傳，確保客戶可以優先取得資料
    return df_all_nm


# ========================== 由前端調用 ==========================
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


# ========================== 由 DAG 調用 ==========================

# 粗篩框的半徑，同時是 aggregate_national_master() 讀取 key 所帶的半徑
RADIUS_KM_ROUGH = 3.0

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


def _build_query_market_batch_nearby_box(
    batch: list[dict], radius_km: float = RADIUS_KM_ROUGH
) -> tuple[str, dict]:
    r"""組出「能涵蓋『整批夜市粗篩方框聯集』」的單一 SQL 查詢語句與其綁定參數。

    - 把一個批次裡面的各個夜市的方框範圍以 OR 做聯集，才做 SQL 查詢。
    因為如果一個夜市的方框範圍獨自做 SQL 查詢的話，高機率發生的是，
    鄰近夜市的事故區域相互重疊，導致一直去 MySQL 重複查詢那些事故，浪費查詢資源。

    - 此外，SQL 查詢語句中的座標一律改走 bind parameter，不以字串內插回查詢。

    Args:
        batch (list[dict]): 一個批次的夜市，每筆須含 `lat` 與 `lon`。
        radius_km (float): 粗篩方框的半徑，單位公里，預設 3.0。

    Returns:
        tuple[str, dict]: 帶具名佔位符的查詢字串與對應的參數字典，形如：

            (
                "SELECT * FROM analysis_pesdestrian_involving_accident \n
                WHERE (latitude BETWEEN :min_lat_0 AND :max_lat_0 AND \n
                             longitude BETWEEN :min_lon_0 AND :max_lon_0) \n
                    OR (latitude BETWEEN :min_lat_1 AND :max_lat_1 AND \n
                             longitude BETWEEN :min_lon_1 AND :max_lon_1)
                ", \n
                {
                    "min_lat_0": 25.061081, "max_lat_0": 25.115118, \n
                    "min_lon_0": 121.497273, "max_lon_0": 121.551327, \n
                    "min_lat_1": 25.001081, ... \n
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
def get_and_slice_nightmarkets_multibatches(
    cache_ttl: int = CACHE_TTL_SECONDS,
) -> list:
    """把夜市清單補齊邊界座標後切成批次存入 Redis，回傳各批的鍵。

    每 30 個夜市一批。夜市若沒有東北、西南兩個邊界端點，就以中心點往該方向
    擴 0.005 度（約 500 公尺）補上。批次資料本身放進 Redis，
    只回傳輕量的鍵字串，避免 DAG 的 XCom 塞進大量資料。

    Args:
        cache_ttl (int): 快取存活秒數，預設為 `CACHE_TTL_SECONDS`（10 天）。

    Returns:
        list[str]: 各批次在 Redis 中的鍵，形如：

            [
                "market:night_markets_batch:3f2a8c1d-...:0",
                "market:night_markets_batch:3f2a8c1d-...:1",
            ]

    Raises:
        RedisError: 讀取或寫入快取失敗。
        SQLAlchemyError: 查詢 MySQL 失敗。
    """
    # 1. 剔除經緯度遺漏 與 經緯度重複
    df_all_nm = get_all_nightmarkets().copy()
    df_all_nm = df_all_nm.dropna(subset=["latitude", "longitude"], how="any")
    df_all_nm = df_all_nm.drop_duplicates(subset=["latitude", "longitude"])

    # 2. 先確保中心點是數值
    # to_numeric 轉換出來的數值型別依照資料本身而定，可能是 float64 也可能是 int64 等，轉不動者填入 NaN
    # 可在 chain 尾端加上 .astype("float64")，強迫數值型別是 float64，float64 有 15-17 位，轉換無損。
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
            # TODO: 有風險，如果"googlemap_rating"欄位不存在，.get()會回傳 0.0 純量，
            # 純量不是 Pandas 專有物件，後接上 fillna() 會噴出 AttributeError
            # 改為 pd.to_numeric(df_all_nm["googlemap_rating"],
            #                   errors="coerce").fillna(0.0).astype("float64")
            # 會更好 debug，因為此時噴 KeyError
            "lat": df_all_nm["latitude"].astype("float64"),
            "lon": df_all_nm["longitude"].astype("float64"),
            "n_lat": n_lat.astype("float64"),
            "s_lat": s_lat.astype("float64"),
            "e_lon": e_lon.astype("float64"),
            "w_lon": w_lon.astype("float64"),
        }
    )

    # 一次性轉成list of dicts
    # TODO: 用 df.iloc 或 np.array_split(df, 10)也可切批次，這句顯得多餘
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
    market_batch_keys = []

    for i, batch in enumerate(batches):
        market_batch_key = f"market:night_markets_batch:{batch_uuid}:{i}"
        set_cache(market_batch_key, batch, cache_ttl)  # 存入Redis
        market_batch_keys.append(market_batch_key)  # 只回傳key給其他函式用

    logger.info(f"已生成 {len(market_batch_keys)} 個Redis keys。")
    return market_batch_keys  # 回傳的只會是 ['market:night_markets_batch:...', ...] 這樣短字串陣列，避免 XCom 爆表問題


# 由 dags/d06_precompute_to_redis 調用
def cal_accidents_nearby_nightmarket(
    cache_key_market_batch: str,
    radius_m_list: list[float | int] | None = None,
    year_targets: list[int | str] | None = None,
    cache_ttl: int = CACHE_TTL_SECONDS,
):
    """計算一個批次內每個夜市周邊的事故，把粗篩與細篩結果寫入 Redis。

    - 一次 SQL 查詢撈回這批夜市方圓 3 公里區域的聯集區域，粗篩出事故區域，
    並且存一份到 Redis 快取，鍵名 `mart:pedestrian_nearby_market:{緯度}_{經度}_3.0_all_sample`。
    此鍵也會讓 `aggregate_national_master()` 讀取的。
    - 接著依半徑與年份的組合執行細篩。
    - 若使用預設的細篩條件，則相當於只寫入粗篩結果到 Redis。

    寫入失敗即 raise。

    Args:
        cache_key_market_batch (str): 批次夜市清單在 Redis 中的鍵，
            即 `get_and_slice_nightmarkets_multibatches()` 回傳的其中一個。
        radius_m_list (list[float | int] | None): 細篩半徑清單，單位公尺，
            預設 `[3000]`。
        year_targets (list[int | str] | None): 細篩年份清單，`"all_sample"`
            代表不分年份，預設 `["all_sample"]`。
        cache_ttl (int): 快取存活秒數，預設為 `CACHE_TTL_SECONDS`（10 天）。

    Returns:
        str: 形如 `"market:night_markets_batch:3f2a8c1d-...:0 處理完成"` 的訊息。

    Raises:
        ValueError: 批次在 Redis 中不存在或為空。
        RedisError: 讀取或寫入快取失敗。
        SQLAlchemyError: 查詢 MySQL 失敗。

    Notes:
        整批一次查詢與快取鍵的契約參考 ADR-0009。
    """
    # 細篩條件，若沒傳入，則以預設值當細篩條件，但其實預設值根本等同於粗篩的條件
    radius_m_list = radius_m_list or [3000]
    year_targets = year_targets or ["all_sample"]

    # 先拿 cache_key 從 Redis 取某一批夜市資料。
    market_batch = get_cache(cache_key_market_batch)
    if not market_batch:
        raise ValueError(
            f"批次 {cache_key_market_batch} 在 Redis 中不存在或為空，無法計算附近事故"
        )

    # 一個批次一次查詢：撈回這 30 個夜市自己的 3 公里區域的聯集，也就是 30 個區域做聯集
    query, params = _build_query_market_batch_nearby_box(market_batch)
    df_market_batch_accidents = get_accident_table_pedestrian_involved_in(query, params)

    valid_cols = [
        c for c in ACCIDENT_MAP_COLUMNS if c in df_market_batch_accidents.columns
    ]
    df_market_batch_accidents = df_market_batch_accidents[valid_cols].copy()

    if not df_market_batch_accidents.empty:
        df_market_batch_accidents["latitude"] = pd.to_numeric(
            df_market_batch_accidents["latitude"], errors="coerce"
        )
        df_market_batch_accidents["longitude"] = pd.to_numeric(
            df_market_batch_accidents["longitude"], errors="coerce"
        )
        df_market_batch_accidents["accident_year"] = pd.to_numeric(
            df_market_batch_accidents["accident_year"], errors="coerce"
        )

    radius_deg_rough = RADIUS_KM_ROUGH / 111  # 1 度大約等於 111 公里。將 3 公里轉成度

    # 先粗篩，遍歷這一批裡面的每個夜市，切出該單一夜市附近 3 公里的方形區域內的事故資料。
    for a_nightmarket in market_batch:  # a_nightmarket: a dict
        # 找單一夜市的經緯度
        nm_lat, nm_lon = a_nightmarket["lat"], a_nightmarket["lon"]

        # 拿著夜市的經緯度畫出方形區域後，從 df_market_batch_accidents 撈出事故資料
        df_market_accidents_rough = df_market_batch_accidents[
            df_market_batch_accidents["latitude"].between(
                nm_lat - radius_deg_rough, nm_lat + radius_deg_rough
            )
            & df_market_batch_accidents["longitude"].between(
                nm_lon - radius_deg_rough, nm_lon + radius_deg_rough
            )
        ].reset_index(
            drop=True
        )  # index 在過濾行為後是不連續的，reset 後將舊的 index 捨棄掉。

        # 這把 key 是 aggregate_national_master() 也需要讀的，所以一定要寫入，否則該函式不能執行
        # 不能取決於呼叫端有沒有把 3000 與 "all_sample" 傳進 radius_m_list／year_targets
        cache_key_rough = f"mart:pedestrian_nearby_market:{nm_lat:.4f}_{nm_lon:.4f}_{RADIUS_KM_ROUGH:.1f}_all_sample"
        set_cache(cache_key_rough, df_market_accidents_rough, cache_ttl)
        logger.info(f"cache_key_rough: {cache_key_rough} 存取成功")

        # 細篩：半徑清單與年份清單，的組合、來計算車禍與夜市的交集
        for radius_m, year in itertools.product(radius_m_list, year_targets):
            r_km = float(radius_m) / 1000.0
            cache_key_market_accidents_refined = f"mart:pedestrian_nearby_market:{nm_lat:.4f}_{nm_lon:.4f}_{r_km:.1f}_{year}"
            # 預設參數下這一圈與粗篩是同一把 key、同一份資料，寫第二次沒有意義
            if cache_key_market_accidents_refined == cache_key_rough:
                continue

            radius_deg_refine = r_km / 111  # 1度大約等於111公里。將公里轉成度

            # 製作過濾條件
            mask = df_market_accidents_rough["latitude"].between(
                nm_lat - radius_deg_refine, nm_lat + radius_deg_refine
            ) & df_market_accidents_rough["longitude"].between(
                nm_lon - radius_deg_refine, nm_lon + radius_deg_refine
            )
            if year != "all_sample":
                mask &= df_market_accidents_rough["accident_year"] == int(year)

            # 跳用條件完成細篩並存入新的dataframe容器
            df_market_accidents_refined = df_market_accidents_rough[mask]

            # 預計算路徑：寫入失敗即 raise
            set_cache(
                cache_key_market_accidents_refined,
                df_market_accidents_refined,
                cache_ttl,
            )

    return f"{cache_key_market_batch} 處理完成"


# 由 dags/d06_precompute_to_redis 調用
def aggregate_national_master(
    batch_keys: list[str], cache_ttl: int = CACHE_TTL_SECONDS
):
    """聚合各批次的預計算結果成全臺總表。

    先用存入 Redis 的批號 key 還原全臺夜市清單，逐夜市讀取上游函式 `cal_accidents_nearby_nightmarket()`
    寫入的 3 公里粗篩快取資料，把落在夜市邊界方框內的事故留下並貼上夜市名稱、
    縣市與評分。接著補上年、季、月、星期、小時等時間欄位，算出 PDI 分數
    （死亡數乘 10 加受傷數乘 2，17 點後與 0 點的事故再乘 1.5）。

    產出全臺總表 `mart:pedestrian_national_master`，完成後刪掉批次鍵，
    釋放 Redis 空間。

    聚合不出任何資料時 raise。

    Args:
        batch_keys (list[str]): 各批次夜市清單在 Redis 中的鍵。
        cache_ttl (int): 快取存活秒數，預設為 `CACHE_TTL_SECONDS`（10 天）。

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

    # 讀取剛剛用 cal_accidents_nearby_nightmarket() 存入 Redis 的夜市周遭車禍資訊，
    # 透過 concat 聚合成全台大表
    all_dfs = []
    for nm in all_night_markets:
        nm_lat, nm_lon = nm["lat"], nm["lon"]

        # 拼出 key
        key = f"mart:pedestrian_nearby_market:{nm_lat:.4f}_{nm_lon:.4f}_3.0_all_sample"
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

        # 在 PDI 計分時這三欄則必須存在
        required_cols = {"accident_hourtime", "death_count", "injury_count"}
        missing_cols = required_cols - set(final_df.columns)
        if missing_cols:
            raise KeyError(f"聚合結果缺少 PDI 計分所需欄位：{sorted(missing_cols)}")

        # 計算PDI分數
        final_df["weight"] = np.where(
            (final_df["Hour"] >= 17) | (final_df["Hour"] == 0), 1.5, 1.0
        )
        final_df["severity"] = (
            final_df["death_count"] * 10 + final_df["injury_count"] * 2
        )
        final_df["pdi_score"] = final_df["severity"] * final_df["weight"]

        # 存入給其他圖表用的原始巨型 DataFrame
        set_cache("mart:pedestrian_national_master", final_df, ttl=cache_ttl)
        logger.info(
            f"全台夜市周邊總表聚合完成，共 {len(final_df)} 筆精準事故，已存入 Redis。"
        )

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
