import redis
import pickle
import pandas as pd
from src.util.create_db_engine_or_database import create_redis_client


def set_cache(key: str, value, ttl=864000) -> None:
    """將資料寫入 Redis，將DataFrame序列化並寫入 Redis
       預設 ttl=864000 (10天)，作為熱資料的有效期限，過期會自動清除以釋放記憶體
    """
    try:
        r = create_redis_client()
        # r = redis.Redis(connection_pool=REDIS_POOL)
        packed_data = pickle.dumps(value)  # 壓縮成二進位
        r.setex(key, ttl, packed_data)
        print(f"[Redis Saved] 已快取: {key}")
    except Exception as e:
        print(f"Redis 寫入失敗: {e}")


def get_cache(key: str) -> dict | pd.DataFrame | None:
    """嘗試從 Redis 讀取並反序列化(Unpickle) 回復成原本的DataFrame/Dict/list。
    """
    try:
        r = create_redis_client()
        # r = redis.Redis(connection_pool=REDIS_POOL)
        data = r.get(key)
        if data:
            print(f"Redis 讀取成功: {key}")
            return pickle.loads(data)  # 解壓縮還原成 DataFrame/Dict/list
    except Exception as e:
        print(f"Redis 讀取失敗: {e}")


def delete_cache(key: str) -> None:
    """強制刪除key，釋放Redis佔用的記憶體"""
    try:
        r = create_redis_client()
        r.delete(key)
    except Exception as e:
        print(f"Redis刪除{key}失敗: {e}")
