"""驗證 create_tables() 的存在性檢查，以及兩支建表任務的 DDL 宣告。"""

import re
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.task.create_night_markets_tables import (
    NIGHT_MARKET_TABLES,
    create_night_market_tables,
)
from src.task.create_traffic_accident_tables import (
    TRAFFIC_ACCIDENT_TABLES,
    create_traffic_accident_tables,
)
from src.util.mysql_utils import create_tables

ALL_DECLARATIONS = {**TRAFFIC_ACCIDENT_TABLES, **NIGHT_MARKET_TABLES}


def _fake_engine():
    """回傳 (engine, conn)；conn 為 engine.begin() 交出的連線。"""
    engine, conn = MagicMock(), MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    return engine, conn


@pytest.mark.parametrize("table_name, ddl", ALL_DECLARATIONS.items())
def test_宣告的鍵與_ddl_實際建立的表同名(table_name, ddl):
    """鍵名與 DDL 不一致時，存在性檢查會永遠落空而重複執行 DDL。"""
    match = re.search(r"CREATE TABLE IF NOT EXISTS\s*`?(\w+)`?", ddl)

    assert match is not None, f"{table_name} 的 DDL 不是 CREATE TABLE IF NOT EXISTS"
    assert match.group(1) == table_name


def test_星狀綱要宣告了四張維度表與三張事實表():
    """對照 CLAUDE.md 記載的資料模型，避免日後漏建。"""
    assert sorted(TRAFFIC_ACCIDENT_TABLES) == [
        "dim_accident_day",
        "dim_accident_type",
        "dim_lane_design",
        "dim_road_design",
        "fact_accident_env",
        "fact_accident_human",
        "fact_accident_main",
    ]
    assert sorted(NIGHT_MARKET_TABLES) == ["fact_night_markets"]


def test_已存在的表被跳過且不執行_ddl():
    """這是改用 create_tables() 的主因：日誌與行為要能區分『已存在』。"""
    engine, conn = _fake_engine()

    with patch("src.util.mysql_utils.inspect_table_exists", return_value=True):
        create_tables(engine, {"dim_accident_day": "CREATE TABLE ..."})

    conn.execute.assert_not_called()
    engine.dispose.assert_not_called()


def test_不存在的表才執行_ddl():
    """不存在時應實際送出 DDL。"""
    engine, conn = _fake_engine()

    with patch("src.util.mysql_utils.inspect_table_exists", return_value=False):
        create_tables(engine, {"dim_accident_day": "CREATE TABLE ..."})

    conn.execute.assert_called_once()
    engine.dispose.assert_not_called()


def test_ddl_失敗時原樣拋出且不關閉共用_engine():
    """依 ADR-0001 不轉換例外型別；依 ADR-0004，共用 Engine 不由呼叫端關閉。"""
    engine, conn = _fake_engine()
    conn.execute.side_effect = SQLAlchemyError("syntax error")

    with patch("src.util.mysql_utils.inspect_table_exists", return_value=False):
        with pytest.raises(SQLAlchemyError):
            create_tables(engine, {"dim_accident_day": "CREATE TABLE ..."})

    engine.dispose.assert_not_called()


def test_建表任務把自己的宣告轉交給_create_tables():
    """兩支任務瘦身後只剩宣告，這份宣告即是它們的全部內容。"""
    engine = MagicMock()

    with patch("src.task.create_traffic_accident_tables.create_tables") as mocked:
        create_traffic_accident_tables(engine)
    mocked.assert_called_once_with(engine, TRAFFIC_ACCIDENT_TABLES)

    with patch("src.task.create_night_markets_tables.create_tables") as mocked:
        create_night_market_tables(engine)
    mocked.assert_called_once_with(engine, NIGHT_MARKET_TABLES)
