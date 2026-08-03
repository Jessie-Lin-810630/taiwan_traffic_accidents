"""業務運算層：夜市周邊事故計算、快取讀寫與全台總表聚合。"""

import itertools
import uuid
from datetime import datetime

import numpy as np
import pandas as pd
from redis.exceptions import RedisError

from src.task.core.c_db import (
    get_accident_table_pedestrian_involved_in,
    get_accident_table_with_main_day,
    get_night_markets_table,
)
from src.util.logger_crtx import get_logger
from src.util.redis_utils import delete_cache, get_cache, set_cache

logger = get_logger(__name__)


# 由app、act1、act2、act3調用
def get_all_nightmarkets() -> pd.DataFrame:
    """讀取全台夜市主檔並清洗經緯度"""
    # 先拿cache_key從Redis取資料
    cache_key = "market:list_all_auto_v3"
    cached = get_cache(cache_key)
    if cached is not None:
        df_cached = pd.DataFrame(cached)  # 轉回df?需這步驟再轉一次嗎？
        if "area_road" in df_cached.columns and "region" in df_cached.columns:
            return df_cached

    # 如果回傳None就改讀MySQL資料庫，並且補存入Redis為下一次讀取加速
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

        # # 綁定四層級：
        # df["adminDistrict"] = df["area_road"]

        # 處理附屬離島特例強制劃分
        df["region"] = df["region"].where(
            df["area_road"].str.contains("琉球|蘭嶼|綠島", na=False), "東部與東部離島"
        )
        df["region"] = df["nightmarket_name"].where(
            df["area_road"].str.contains("琉球|蘭嶼|綠島", na=False), "東部與東部離島"
        )
        # df.loc[df["adminDistrict"].str.contains("琉球|蘭嶼|綠島", na=False), "region"] = "東部與東部離島"
        # df.loc[df["name"].str.contains("琉球|蘭嶼|綠島", na=False), "region"] = "東部與東部離島"

        # 剔除經緯度遺漏的髒資料 (正常來說不會有)
        df_all_nm = df.dropna(subset=["latitude", "longitude"], how="any")
    except Exception:
        # 本層只是轉手，僅記錄發生什麼；traceback 由邊界層帶 exc_info 輸出
        logger.error("夜市事實表從 MySQL 讀取失敗")
        raise

    # read-through 快取：資料已取得，寫入失敗不影響本次回傳（ADR-0003 子決策 2）
    try:
        result = df_all_nm.to_dict("records")
        set_cache(cache_key, result, ttl=43200)
    except RedisError:
        # 本層即為例外停止傳播之處，故完整記錄（ADR-0003）
        logger.warning(f"快取寫入失敗，不影響本次回傳: {cache_key}", exc_info=True)

    # 無論Redis寫入是否成功，只要MySQL有拿到資料就回傳，確保客戶可以優先取得資料
    return df_all_nm


# 由dag_precompute調用
def get_and_slice_nightmarkets_multibatches() -> list:  # fetch_and_split_markets原名
    """將夜市地理資訊補值後，拆成多個batch，分批存入Redis"""
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


# 由act1調用
def haversine_distance(
    lat1: float | int, lon1: float | int, lat2: float | int, lon2: float | int
) -> float:
    """利用Haversine 半正矢公式 計算地球表面兩點之間的大圓距離 (unit: km)。

    特點是使用 numpy 進行向量化運算，比寫 for 迴圈逐筆算快上幾百倍。
    p.s., R=6371 為地球半徑(km)
    """
    R = 6371
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


# 由act3調用
def get_pedestrian_stats_by_region_monthly():
    """依地區與年月統計行人事故件數。"""
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
    # read-through 快取：資料已取得，寫入失敗不影響本次回傳（ADR-0003 子決策 2）
    try:
        result = df.to_dict("records")
        set_cache(cache_key, result, ttl=43200)
    except RedisError:
        # 本層即為例外停止傳播之處，故完整記錄（ADR-0003）
        logger.warning(f"快取寫入失敗，不影響本次回傳: {cache_key}", exc_info=True)

    # 無論Redis寫入是否成功，只要MySQL有拿到資料就回傳，確保客戶可以優先取得資料
    return df


# 由act3調用
def get_pedestrian_trend(lat=None, lon=None, radius_km=0.5):
    """統計行人事故的逐月趨勢；給定經緯度時只計算該半徑範圍內。"""
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
    # read-through 快取：資料已取得，寫入失敗不影響本次回傳（ADR-0003 子決策 2）
    try:
        result = df.to_dict("records")
        set_cache(cache_key, result, ttl=43200)
    except RedisError:
        # 本層即為例外停止傳播之處，故完整記錄（ADR-0003）
        logger.warning(f"快取寫入失敗，不影響本次回傳: {cache_key}", exc_info=True)

    # 無論Redis寫入是否成功，只要MySQL有拿到資料就回傳，確保客戶可以優先取得資料
    return df


# 由app調用
def get_accident_heatmap_data(sample_size: int = 8000):
    """事故熱點圖資料統計"""
    # 先拿cache_key從Redis取資料
    cache_key = "traffic:global_heatmap_lite_v2"
    cached = get_cache(cache_key)
    if cached:
        df_cached = pd.DataFrame(cached)
        return df_cached

    # 如果回傳None就改讀MySQL資料庫，並且補存入Redis為下一次讀取加速
    try:
        df = get_accident_table_with_main_day()
        if df.empty:
            return []

        df = df.dropna(subset=["latitude", "longitude"])
        df["latitude"] = df["latitude"].astype("float64")
        df["longitude"] = df["longitude"].astype("float64")

        # 踢掉不在台灣國土範圍內的奇怪經緯度 (大概可減少100個點)
        df = df[
            (25.93916 > df["latitude"])
            & (df["latitude"] > 21.755)
            & (124.56916 > df["longitude"])
            & (df["longitude"] > 119.30083)
        ]

        # 若需要，僅撈取同一座標重複發生 3 次以上的熱點，大幅縮小前端記憶體消耗
        df = df.groupby(by=["latitude", "longitude"]).size().reset_index(name="count")
        df = df[df["count"] >= 3]

        # 如果熱力圖資料點仍超過預設8000點 (sample_size)，強制隨機抽樣，避免地圖卡頓
        if len(df) > sample_size:
            df = df.sample(n=sample_size, random_state=42)

    except Exception:
        # 本層只是轉手，僅記錄發生什麼；traceback 由邊界層帶 exc_info 輸出
        logger.error("熱點圖資料從 MySQL 讀取失敗")
        raise
    # read-through 快取：資料已取得，寫入失敗不影響本次回傳（ADR-0003 子決策 2）
    try:
        result = df.to_dict("records")
        set_cache(cache_key, result, ttl=43200)
    except RedisError:
        # 本層即為例外停止傳播之處，故完整記錄（ADR-0003）
        logger.warning(f"快取寫入失敗，不影響本次回傳: {cache_key}", exc_info=True)

    # 無論Redis寫入是否成功，只要MySQL有拿到資料就回傳，確保客戶可以優先取得資料
    return df


# 粗篩框的半徑，同時是 aggregate_national_master() 讀取的契約 key 所帶的半徑
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
    """組出涵蓋整批夜市粗篩框「聯集」的單一查詢與其綁定參數（ADR-0009）。

    查詢的粒度必須與使用它的粒度一致：本函式的呼叫端服務的是一個批次，
    查詢就該是一句。逐夜市各發一次的話，同一個都會區的鄰近夜市會把
    重疊區域的事故重複撈回來，而且被查的分析表是用 `CREATE TABLE ... AS SELECT`
    生出來的、沒有索引（那種建表方式只複製資料不複製索引），
    每一次都是一趟全表掃描。

    座標一律走 bind parameter，不以 f-string 內插回查詢字串。

    Parameters:
        batch (list[dict]): 一個批次的夜市，每筆須含 `lat` 與 `lon`。
        radius_km (float): 粗篩方框的半徑（公里）。

    Returns:
        tuple[str, dict]: 帶具名佔位符的 DQL，以及對應的參數字典。
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


# 由dag_precompute調用，預計要依賴get_and_slice_nightmarkets_multibatches()
def cal_accidents_nearby_nightmarket(
    batch_key: str,
    radius_m_list: list[float | int] | None = None,
    year_targets: list[int | str] | None = None,
):  # process_market_batch()原名
    """計算一個批次內每個夜市周邊的事故，並將粗篩與細篩結果寫入 Redis。

    整個批次只發一次查詢（ADR-0009），因此查詢失敗即整批失敗 ——
    一次查詢服務整批，它失敗就是整批沒有資料，沒有部分成功可言。
    """
    radius_m_list = radius_m_list or [3000]
    year_targets = year_targets or ["all_sample"]

    # 先拿cache_key從Redis取夜市資料。
    # Redis 故障會直接拋出 RedisError（ADR-0003），因此 None 只代表快取未命中。
    batch = get_cache(batch_key)
    if not batch:
        raise ValueError(f"批次 {batch_key} 在 Redis 中不存在或為空，無法計算附近事故")

    # 一個批次一次查詢：撈回這 30 個夜市 3 公里框的聯集（ADR-0009）
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

        # 預計算路徑：快取本身就是產出，寫入失敗即 raise（ADR-0003 子決策 2）
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

            # 預計算路徑：寫入失敗即 raise（ADR-0003 子決策 2）
            set_cache(cache_key_per_product, df_target, 43200)

    return f"{batch_key} 處理完成"


# 由dag_precompute調用，預計要依賴cal_accidents_nearby_nightmarket()
def aggregate_national_master(batch_keys: list[dict]):
    """憑所有號碼牌還原全台清單，聚合總表並產出 Audit 輕量報表"""
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

        # ====存入給其他圖表用的原始巨型 DataFrame====
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
        # 將 300 個夜市分開存成各自的 Key，讓前端地圖點擊時可以「秒拉」資料
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
        # 不能靜默結束讓 task 顯示成功（ADR-0003 子決策 1）
        raise RuntimeError(
            f"無法聚合全台總表：{len(batch_keys)} 個批次共 "
            f"{len(all_night_markets)} 個夜市，皆未找到周邊事故快取"
        )
