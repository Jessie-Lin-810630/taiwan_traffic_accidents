"""驗證 ADR-0008：必填設定在「真正要用」的那一刻拋出，訊息直指設定名稱。"""

import importlib
from unittest.mock import MagicMock, patch

import pytest

from src.task import e_crawling_nightmarket
from src.util import mysql_utils, redis_utils

# --- MySQL：三個連線入口共用同一支檢查 ---------------------------------------


@pytest.mark.parametrize(
    "missing, expected",
    [("username", "MYSQL_USER"), ("password", "MYSQL_PASSWORD")],
)
def test_mysql_缺設定時訊息直指設定名稱(monkeypatch, missing, expected):
    """原本會走到「Access denied for user 'None'」，排查方向被帶往帳號權限。"""
    monkeypatch.setattr(mysql_utils, "username", "test_user")
    monkeypatch.setattr(mysql_utils, "password", "test_password")
    monkeypatch.setattr(mysql_utils, missing, None)

    with pytest.raises(ValueError, match=expected):
        mysql_utils._create_engine("traffic_accidents")


@pytest.mark.parametrize(
    "entry_point",
    [
        "_create_engine",
        "get_pymysql_conn_to_mysql",
        "get_pymysql_conn_to_mysql_multistatement",
    ],
)
def test_三個_mysql_連線入口都會驗(monkeypatch, entry_point):
    """帳密由三個入口共用，漏掉任何一個都會讓錯誤訊息回到原樣。"""
    monkeypatch.setattr(mysql_utils, "username", None)
    monkeypatch.setattr(mysql_utils, "password", "test_password")

    with pytest.raises(ValueError, match="MYSQL_USER"):
        getattr(mysql_utils, entry_point)("traffic_accidents")


def test_mysql_設定齊全時不擋():
    """有帳密就該放行 —— 驗證不能變成阻礙。"""
    with (
        patch("src.util.mysql_utils.username", "u"),
        patch("src.util.mysql_utils.password", "p"),
    ):
        mysql_utils._require_credentials()  # 不得拋出


# --- Redis ------------------------------------------------------------------


def test_redis_缺密碼時訊息直指設定名稱(monkeypatch):
    """放任 None 往下走的話，redis 只會回 NOAUTH，看不出是設定缺失。"""
    monkeypatch.setattr(redis_utils, "redis_password", None)
    monkeypatch.setattr(redis_utils, "_REDIS_POOL", None)

    with pytest.raises(ValueError, match="REDIS_PASSWORD"):
        redis_utils._get_redis_pool()


# --- Google Places API ------------------------------------------------------


@pytest.mark.parametrize("func_name", ["search_place_id", "get_place_details"])
def test_兩支_api_函式都會驗金鑰(monkeypatch, func_name):
    """原本只有 e_crawling_nightmarket() 有檢查，兩支 API 函式沒有。"""
    monkeypatch.setattr(e_crawling_nightmarket, "API_KEY", None)

    with pytest.raises(ValueError, match="GOOGLE_MAP_API_KEY"):
        getattr(e_crawling_nightmarket, func_name)("士林夜市")


def test_缺金鑰時不會先送出請求(monkeypatch):
    """驗證要在呼叫 API 之前，否則等於白花一次配額。"""
    monkeypatch.setattr(e_crawling_nightmarket, "API_KEY", None)

    with patch(
        "src.task.e_crawling_nightmarket.requests.get", return_value=MagicMock()
    ) as fake_get:
        with pytest.raises(ValueError):
            e_crawling_nightmarket.search_place_id("士林夜市")

    fake_get.assert_not_called()


# --- 驗證的時機：使用時，而非 import 時 --------------------------------------


@pytest.mark.parametrize(
    "module, attrs",
    [
        (mysql_utils, ("username", "password")),
        (redis_utils, ("redis_password",)),
        (e_crawling_nightmarket, ("API_KEY",)),
    ],
)
def test_缺設定不影響模組匯入(monkeypatch, module, attrs):
    """模組層 raise 會讓 pytest 與 Airflow 的 DAG parse 在缺設定時全面陣亡。

    127 個測試中多數只測 SQL 組裝、不需真連線，不該因為少一個環境變數而無法載入。
    """
    for attr in attrs:
        monkeypatch.setattr(module, attr, None)

    assert importlib.import_module(module.__name__) is not None
