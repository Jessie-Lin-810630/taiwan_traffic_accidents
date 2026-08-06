"""Redis 的唯一存取面：連線池單例與 Pickle 快取的讀寫刪除。

專案用 Redis 存放 DAG 預先算好的運算結果，讓前端頁面
直接讀快取而不重算。存取一律將運算結果導出的 DataFrame、dict 走 pickle 序列化，
因此連線固定 `decode_responses=False`，避免二進位資料被當成字串解碼而損毀。

連線設定取自環境變數:

- 有預設值: `REDIS_HOST`（localhost）、`REDIS_PORT`（6379）
- 必填: `REDIS_PASSWORD`，缺少時在建立連線池的那一刻拋出

Notes:
    必填環境變數的檢查時機參考 ADR-0008。
"""

import os
import pickle

import pandas as pd
import redis
from dotenv import load_dotenv
from redis.exceptions import RedisError

from src.util.logger_crtx import get_logger

logger = get_logger(__name__)

load_dotenv()
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = os.getenv("REDIS_PORT", 6379)
redis_password = os.getenv("REDIS_PASSWORD")


# 全域單例連線池（Singleton）：避免每次呼叫都重複建立連線池導致連線數爆炸
_REDIS_POOL = None


def _get_redis_pool() -> redis.ConnectionPool:
    """取得全域唯一的 Redis 連線池，首次呼叫時才建立。

    以模組層變數保存單例，避免每次取用都新建連線池而讓連線數暴增。
    池的設定固定不解碼回應（保護 pickle 二進位資料）、連線逾時 3 秒、
    讀寫逾時 5 秒。

    Returns:
        redis.ConnectionPool: 連往 Redis 伺服器的連線池。

    Raises:
        ValueError: `REDIS_PASSWORD` 未設定。

    Notes:
        參考 ADR-0008。
    """
    if not redis_password:
        raise ValueError("未設定 REDIS_PASSWORD，請檢查環境變數設置")

    global _REDIS_POOL
    if _REDIS_POOL is None:
        _REDIS_POOL = redis.ConnectionPool(
            host=redis_host,
            port=int(redis_port),
            password=redis_password,
            decode_responses=False,  # 硬編碼 False，確保 Pickle 二進位資料不損壞
            socket_timeout=5,  # 讀寫超時設定，pickled dataframe 資料設定 3 - 5 秒
            socket_connect_timeout=3,  # 連線超時設定，內網 VPC、不跨機房可以試看看 0.1 - 0.5 秒
        )
    return _REDIS_POOL


def create_redis_client() -> redis.Redis:
    """從連線池取得 Redis 用戶端，並先 ping 過確認連線可用。

    連線問題會在這裡就被發現，而不是延到實際讀寫時才浮現；出錯時記下 error
    並原樣往外拋，保留完整 traceback 供呼叫端追查。

    Returns:
        redis.Redis: 已通過連線測試的 Redis 用戶端。

    Raises:
        ValueError: `REDIS_PASSWORD` 未設定。
        RedisError: 連線失敗或 ping 無回應。
        Exception: 其他非預期錯誤，同樣記下 error 後原樣往外拋。
    """
    logger.info("==== Connecting to Redis Server... ====")

    try:
        # 1. 從全域連線池獲取連線
        pool = _get_redis_pool()
        r = redis.Redis(connection_pool=pool)

        # 2. 測試連線是否有效
        r.ping()
    except RedisError:
        logger.error("Redis connection error occurred.")
        raise

    except Exception:
        logger.error("Unexpected error when creating Redis client.")
        raise

    else:
        logger.info("==== Successfully connected to Redis! ====")
        return r


def set_cache(key: str, value, ttl: int = 864000) -> None:
    """把物件序列化後寫入 Redis，並設定存活時間 (TTL)。

    值以 pickle 序列化再寫入，因此 DataFrame、dict、list 都能直接存。
    超過存活時間後由 Redis 自動刪除，避免舊快取一直佔用記憶體。

    Args:
        key (str): 快取的鍵名。
        value: 要存入的資料，須為 pickle 可序列化的物件。
        ttl (int): 存活秒數，預設 864000 秒（10 天）。

    Raises:
        ValueError: `REDIS_PASSWORD` 未設定。
        RedisError: 連線或寫入失敗。
        Exception: 其他非預期錯誤，例如物件無法被 pickle 序列化。
    """
    try:
        r = create_redis_client()
        packed_data = pickle.dumps(value)
        r.setex(key, ttl, packed_data)
        logger.info(f"Wrote and saved cache in Redis with key name: {key}")

    except RedisError:
        logger.error(f"Redis write error for key '{key}'.")
        raise

    except Exception:
        logger.error(f"Unexpected error when writing to Redis for key '{key}'.")
        raise


def get_cache(key: str) -> dict | pd.DataFrame | None:
    """從 Redis 讀出快取並還原成原本的物件。

    讀回的位元組以 `pickle.loads()` 還原，因此回傳型別取決於當初存進去的是什麼。
    鍵不存在或已過期會回傳 `None`，與「Redis 故障」區分開來 —— 後者是拋例外。

    Args:
        key (str): 快取的鍵名。

    Returns:
        dict | pandas.DataFrame | None: 還原後的快取物件；鍵不存在或已過期時為 `None`。
            以夜市周邊事故的快取為例，還原出的 DataFrame 形如：

                night_market  accident_id  distance_km  deaths
                士林夜市      1130101001   0.42         0
                士林夜市      1130101002   1.87         1

    Raises:
        ValueError: `REDIS_PASSWORD` 未設定。
        RedisError: 連線或讀取失敗。
        Exception: 其他非預期錯誤，例如反序列化失敗。
    """
    try:
        r = create_redis_client()
        data = r.get(key)
        if data:
            logger.info(f"Got key of cache data from Redis: {key}")
            return pickle.loads(data)
        return None

    except RedisError:
        logger.error(f"Redis read error for key '{key}'.")
        raise

    except Exception:
        logger.error(f"Unexpected error when reading from Redis for key '{key}'.")
        raise


def delete_cache(key: str) -> None:
    """立即刪除指定的快取鍵，釋放其佔用的記憶體。

    鍵本來就不存在時不算失敗，Redis 直接回報刪除 0 筆。

    Args:
        key (str): 要刪除的快取鍵名。

    Raises:
        ValueError: `REDIS_PASSWORD` 未設定。
        RedisError: 連線或刪除失敗。
        Exception: 其他非預期錯誤。
    """
    try:
        r = create_redis_client()
        r.delete(key)
        logger.info(f"Deleted the cache in Redis with key name: {key}")

    except RedisError:
        logger.error(f"Redis delete error for key '{key}'.")
        raise

    except Exception:
        logger.error(f"Unexpected error when deleting from Redis for key '{key}'.")
        raise
