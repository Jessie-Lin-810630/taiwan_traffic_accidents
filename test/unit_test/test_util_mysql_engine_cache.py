"""驗證 ADR-0004：MySQL Engine 以資料庫名為鍵的單例快取。"""

from unittest.mock import MagicMock, patch

import pytest

from src.util import mysql_utils


@pytest.fixture(autouse=True)
def _clear_engine_cache():
    """每個測試前後清空模組層快取，避免跨測試汙染。"""
    mysql_utils._ENGINES.clear()
    yield
    mysql_utils._ENGINES.clear()


def test_同一資料庫重複呼叫回傳同一個_engine():
    """快取命中時不得重建 Engine —— 這是本 ADR 的核心行為。"""
    first = mysql_utils.get_engine_to_mysql("traffic_accidents")
    second = mysql_utils.get_engine_to_mysql("traffic_accidents")

    assert first is second


def test_不同資料庫各自持有獨立的_engine():
    """快取鍵是資料庫名，不同資料庫不可共用同一個連線池。"""
    traffic = mysql_utils.get_engine_to_mysql("traffic_accidents")
    airflow = mysql_utils.get_engine_to_mysql("airflow_db")

    assert traffic is not airflow


def test_未指定資料庫的_none_也會被快取():
    """d01 建立資料庫時傳 None，該情境同樣不該每次重建。"""
    first = mysql_utils.get_engine_to_mysql()
    second = mysql_utils.get_engine_to_mysql(None)

    assert first is second
    assert None in mysql_utils._ENGINES


def test_快取命中時不再呼叫_create_engine():
    """直接釘住「只建立一次」，而非僅比對物件同一性。"""
    with patch(
        "src.util.mysql_utils._create_engine", return_value=MagicMock()
    ) as fake_create:
        for _ in range(5):
            mysql_utils.get_engine_to_mysql("traffic_accidents")

    fake_create.assert_called_once_with("traffic_accidents")


def test_多次查詢只建立一個_engine():
    """熱路徑保證：d06 迴圈內逐個夜市查詢不應各建一個連線池。"""
    engine = MagicMock()
    result = engine.connect.return_value.__enter__.return_value.execute.return_value
    result.fetchall.return_value = []
    result.keys.return_value = ["accident_id"]

    with patch(
        "src.util.mysql_utils._create_engine", return_value=engine
    ) as fake_create:
        for _ in range(300):
            mysql_utils.get_table_from_sqlserver(
                "SELECT * FROM fact_accident_main", database="traffic_accidents"
            )

    fake_create.assert_called_once()
    assert engine.connect.call_count == 300


def test_建表函式不再_dispose_共用的_engine():
    """Engine 所有權在 mysql_utils，呼叫端關閉它等於丟棄整個池。"""
    engine = MagicMock()

    mysql_utils.create_tables(engine, {})
    mysql_utils.create_database(engine, "traffic_accidents")

    engine.dispose.assert_not_called()


def test_舊版_create_engine_to_mysql_已不存在():
    """舊名會誤導維護者以為每次拿到新 Engine，故不保留別名。"""
    assert not hasattr(mysql_utils, "create_engine_to_mysql")


def test_舊模組已刪除():
    """get_table_from_sql_server 的實作已併入 mysql_utils，不得留下第二份。"""
    with pytest.raises(ImportError):
        from src.util import get_table_from_sql_server  # noqa: F401
