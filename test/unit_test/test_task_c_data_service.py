"""驗證資料服務層不再吞噬例外（ADR-0003）。"""

from unittest.mock import patch

import pandas as pd
import pytest
from redis.exceptions import RedisError

import src.task.core.c_data_service as ds

NIGHTMARKET_ROW = {
    "name": "士林夜市",
    "lat": 25.0878,
    "lon": 121.5240,
    "n_lat": 25.09,
    "s_lat": 25.08,
    "e_lon": 121.53,
    "w_lon": 121.52,
    "city": "臺北市",
    "rating": 4.2,
}


class TestGetAllNightmarkets:
    """read-through 快取：MySQL 故障要拋出，Redis 寫入失敗只降級。"""

    def test_mysql_故障時拋出而非回傳空表(self):
        """MySQL 連線失敗應原樣拋出，而非以空表掩蓋。"""
        with (
            patch.object(ds, "get_cache", return_value=None),
            patch.object(
                ds, "get_night_markets_table", side_effect=OSError("connection refused")
            ),
        ):
            with pytest.raises(OSError, match="connection refused"):
                ds.get_all_nightmarkets()

    def test_redis_寫入失敗不影響回傳(self):
        """資料已取得，快取只是加速手段（ADR-0003 子決策 2 的 read-through 路徑）。"""
        df = pd.DataFrame(
            {
                "latitude": [25.0],
                "longitude": [121.5],
                "northeast_latitude": [25.1],
                "northeast_longitude": [121.6],
                "southwest_latitude": [24.9],
                "southwest_longitude": [121.4],
                "area_road": ["士林區"],
                "region": ["北部"],
                "nightmarket_name": ["士林夜市"],
            }
        )
        with (
            patch.object(ds, "get_cache", return_value=None),
            patch.object(ds, "get_night_markets_table", return_value=df),
            patch.object(ds, "set_cache", side_effect=RedisError("down")),
        ):
            result = ds.get_all_nightmarkets()

        assert not result.empty


class TestCalAccidentsNearbyNightmarket:
    """預計算路徑：一批一次查詢（ADR-0009），失敗即整批失敗。"""

    def test_批次不存在時拋出而非回傳字串(self):
        """批次不存在代表上游未產出，不可當成正常結果。"""
        with patch.object(ds, "get_cache", return_value=None):
            with pytest.raises(ValueError, match="無法計算附近事故"):
                ds.cal_accidents_nearby_nightmarket(
                    "market:night_markets_batch:missing"
                )

    def test_查詢失敗時整批拋出(self):
        """一次查詢服務整批，它失敗就是整批沒資料，沒有部分成功可言（ADR-0009 子決策 6）。"""
        with (
            patch.object(ds, "get_cache", return_value=[NIGHTMARKET_ROW]),
            patch.object(
                ds,
                "get_accident_table_pedestrian_involved_in",
                side_effect=OSError("mysql down"),
            ),
            patch.object(ds, "set_cache"),
        ):
            with pytest.raises(OSError, match="mysql down"):
                ds.cal_accidents_nearby_nightmarket("batch_key")

    def test_全部成功時回傳完成訊息(self):
        """沒有失敗時維持原本的回傳值。"""
        df = pd.DataFrame(
            {
                "accident_id": ["A1"],
                "latitude": [25.0878],
                "longitude": [121.5240],
                "accident_year": [2025],
            }
        )
        with (
            patch.object(ds, "get_cache", return_value=[NIGHTMARKET_ROW]),
            patch.object(
                ds, "get_accident_table_pedestrian_involved_in", return_value=df
            ),
            patch.object(ds, "set_cache"),
        ):
            result = ds.cal_accidents_nearby_nightmarket("batch_key")

        assert "處理完成" in result

    def test_快取寫入失敗直接拋出(self):
        """預計算的產出就是快取，寫不進去等於失敗（ADR-0003 子決策 2）。"""
        df = pd.DataFrame(
            {
                "accident_id": ["A1"],
                "latitude": [25.0878],
                "longitude": [121.5240],
                "accident_year": [2025],
            }
        )
        with (
            patch.object(ds, "get_cache", return_value=[NIGHTMARKET_ROW]),
            patch.object(
                ds, "get_accident_table_pedestrian_involved_in", return_value=df
            ),
            patch.object(ds, "set_cache", side_effect=RedisError("down")),
        ):
            with pytest.raises(RedisError):
                ds.cal_accidents_nearby_nightmarket("batch_key")


class TestAggregateNationalMaster:
    """聚合不出任何資料代表上游預計算未生效，不可靜默成功。"""

    def test_聚合不到資料時拋出(self):
        """聚合不到任何資料代表上游預計算未生效。"""
        with patch.object(ds, "get_cache", return_value=None):
            with pytest.raises(RuntimeError, match="無法聚合全台總表"):
                ds.aggregate_national_master(["batch_0"])


def test_資料服務層不再匯入已刪除的舊版快取工具():
    """凍結三檔已於本輪刪除，任何殘留匯入都會在此失敗。"""
    assert not hasattr(ds, "create_redis_client")

    import src.util.redis_utils as ru

    assert ds.get_cache is ru.get_cache
    assert ds.set_cache is ru.set_cache
    assert ds.delete_cache is ru.delete_cache


def test_舊版三檔已不存在():
    """ADR-0001 決策 5 的凍結批次，前提條件滿足後整批刪除。"""
    import importlib.util

    for name in (
        "src.util.create_db_engine_or_database",
        "src.util.get_or_set_cache_from_redis",
        "src.util.inspect_table_schema",
    ):
        assert importlib.util.find_spec(name) is None, f"{name} 應已刪除"
