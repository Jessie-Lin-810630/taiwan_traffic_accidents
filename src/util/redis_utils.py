"""Redis 共用工具：連線池單例、Pickle 快取的讀寫與刪除。"""

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
REDIS_POOL = None


def _get_redis_pool() -> redis.ConnectionPool:
    """初始化並獲取全域的 Redis 連線池。

    針對要存入 Redis 資料庫這類 Pickled data 二進位資料的目標。

    Returns:
        redis.ConnectionPool: Connection Pool to Redis Server
    """
    global REDIS_POOL
    if REDIS_POOL is None:
        REDIS_POOL = redis.ConnectionPool(
            host=redis_host,
            port=int(redis_port),
            password=redis_password,
            decode_responses=False,  # 硬編碼 False，確保 Pickle 二進位資料不損壞
            socket_timeout=5,  # 讀寫超時設定，pickled dataframe 資料設定 3 - 5 秒
            socket_connect_timeout=3,  # 連線超時設定，內網 VPC、不跨機房可以試看看 0.1 - 0.5 秒
        )
    return REDIS_POOL


def create_redis_client() -> redis.Redis:
    """建立並測試 Redis 用戶端連線。

    固定使用 decode_responses=False 以支援 Pickle 二進位資料存取。
    在出錯時會主動向上拋出原始錯誤，保留完整 traceback 供呼叫端追查。

    Returns:
        redis.Redis: new Redis client
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
    """Store a serialized object in Redis with a configurable expiration time.

    The object is serialized using pickle before being written to Redis.
    Cached data expires automatically after the specified TTL, helping
    prevent stale data accumulation and reducing memory usage.

    Parameters:
        key (str): name of key to represeting the saved cache
        value (Any): data to be saved as cache.
        ttl (int, `default = 86400 seconds`): time-to-live in seconds of cache in Redis. If the storage time exceeds ttl, the cache will be delete to release the memory.
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
    """Retrieve and deserialize a cached object from Redis.

    The cached value is loaded from Redis and deserialized using
    ``pickle.loads()`` before being returned. If the specified key does not exist or has expired, ``None`` is returned.

    Parameters:
        key (str): Redis key associated with the cached object.

    Returns:
        dict | pd.DataFrame | None: The deserialized cached object if the key exists; otherwise ``None``. The returned object type depends on what was originally stored (e.g., pandas DataFrame, dict, list, or other pickle-serializable objects).
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
    """Delete a cached object from Redis.

    Removes the specified Redis key immediately, allowing the associated
    cached data and memory usage to be released.

    :params key: Redis key associated with the cached object to delete.
    :type key: str
    :raises RedisError: If the Redis delete operation fails.
    :raises Exception: For any unexpected error during the deletion process.
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
