"""驗證 upsert_to_table 組出的 SQL、事務行為與資源釋放。"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pymysql
import pytest

from src.util.mysql_utils import upsert_to_table

DF = pd.DataFrame({"day_id": [1, 2], "accident_weekday": ["星期一", "星期二"]})


def _run(df=DF, **kwargs):
    """以假連線執行 upsert_to_table，回傳 (conn, cursor) 供斷言。"""
    conn, cursor = MagicMock(), MagicMock()
    conn.cursor.return_value = cursor
    params = {"table": "dim_accident_day", "update_columns": ["accident_weekday"]}
    params.update(kwargs)
    with patch("src.util.mysql_utils.get_pymysql_conn_to_mysql", return_value=conn):
        upsert_to_table(df, **params)
    return conn, cursor


def test_組出的_sql_包含表名與_upsert_子句():
    """表名與 update 欄位應正確嵌入 INSERT ... ON DUPLICATE KEY UPDATE。"""
    _, cursor = _run()
    sql = cursor.executemany.call_args[0][0]

    assert "INSERT INTO dim_accident_day (day_id, accident_weekday)" in sql
    assert "VALUES (%s, %s)" in sql
    assert "ON DUPLICATE KEY UPDATE accident_weekday=VALUES(accident_weekday)" in sql


def test_多個_update_欄位以逗號串接():
    """update_columns 有多個時，應組成 col=VALUES(col) 並以逗號分隔。"""
    _, cursor = _run(update_columns=["updated_on", "business_hours_opening"])
    sql = cursor.executemany.call_args[0][0]

    assert (
        "ON DUPLICATE KEY UPDATE updated_on=VALUES(updated_on), "
        "business_hours_opening=VALUES(business_hours_opening)" in sql
    )


def test_傳入的資料列與_dataframe_一致():
    """Executemany 應收到 DataFrame 轉出的 list of lists。"""
    _, cursor = _run()

    assert cursor.executemany.call_args[0][1] == [[1, "星期一"], [2, "星期二"]]


def test_成功時提交且釋放資源():
    """正常路徑應 commit，並關閉 cursor 與 conn。"""
    conn, cursor = _run()

    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()
    cursor.close.assert_called_once()
    conn.close.assert_called_once()


def test_寫入失敗時復原事務並原樣拋出():
    """依 ADR-0001，不轉換例外型別；且必須 rollback。"""
    conn, cursor = MagicMock(), MagicMock()
    conn.cursor.return_value = cursor
    cursor.executemany.side_effect = pymysql.MySQLError("duplicate entry")

    with patch("src.util.mysql_utils.get_pymysql_conn_to_mysql", return_value=conn):
        with pytest.raises(pymysql.MySQLError):
            upsert_to_table(
                DF, table="dim_accident_day", update_columns=["accident_weekday"]
            )

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once()


def test_取得游標失敗不會被_attributeerror_遮蔽():
    """conn.cursor() 失敗時，finally 不得因 cursor 為 None 而拋 AttributeError。"""
    conn = MagicMock()
    conn.cursor.side_effect = pymysql.MySQLError("too many connections")

    with patch("src.util.mysql_utils.get_pymysql_conn_to_mysql", return_value=conn):
        with pytest.raises(pymysql.MySQLError, match="too many connections"):
            upsert_to_table(
                DF, table="dim_accident_day", update_columns=["accident_weekday"]
            )

    conn.close.assert_called_once()


def test_update_columns_為空時拒絕執行():
    """空的 update_columns 會組出語法錯誤的 SQL，應提前擋下。"""
    with pytest.raises(ValueError, match="update_columns"):
        upsert_to_table(DF, table="dim_accident_day", update_columns=[])
