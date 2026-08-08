"""驗證 update_table 組出的 SQL、佔位符順序、型別轉換與事務行為。"""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pymysql
import pytest

from src.util.mysql_utils import update_table

DF = pd.DataFrame(
    {
        "accident_id": ["A1", "A2"],
        "weather_record_id": np.array([1024, 2371], dtype="int64"),
    }
)


def _run(df=DF, **kwargs):
    """以假連線執行 update_table，回傳 (conn, cursor) 供斷言。"""
    conn, cursor = MagicMock(), MagicMock()
    conn.cursor.return_value = cursor
    params = {
        "table": "fact_accident_main",
        "where_columns": ["accident_id"],
        "set_columns": ["weather_record_id"],
    }
    params.update(kwargs)
    with patch("src.util.mysql_utils.get_pymysql_conn_to_mysql", return_value=conn):
        update_table(df, **params)
    return conn, cursor


def test_組出的_sql_包含表名與_set_where_子句():
    """表名、set 欄位與 where 欄位應正確嵌入 UPDATE 敘述。"""
    _, cursor = _run()
    sql = cursor.executemany.call_args[0][0]

    assert "UPDATE fact_accident_main SET weather_record_id=%s" in sql
    assert "WHERE (accident_id=%s)" in sql


def test_多個_set_欄位以逗號串接而_where_以_and_串接():
    """Set 是逗號、where 是 AND —— 兩者串接方式不同，不可寫反。"""
    df = pd.DataFrame(
        {
            "day_id": [1],
            "accident_time": ["08:00:00"],
            "weather_record_id": [7],
            "death_count": [0],
        }
    )
    _, cursor = _run(
        df,
        where_columns=["day_id", "accident_time"],
        set_columns=["weather_record_id", "death_count"],
    )
    sql = cursor.executemany.call_args[0][0]

    assert "SET weather_record_id=%s, death_count=%s" in sql
    assert "WHERE (day_id=%s AND accident_time=%s)" in sql


def test_資料順序為先_set_後_where():
    """佔位符的順序是 SET 在前 WHERE 在後，欄位順序必須跟著對齊。

    釘住的是：df 的欄位順序（accident_id 在前）不得影響傳給 executemany 的值，
    否則會拿 accident_id 去更新 weather_record_id。
    """
    _, cursor = _run()

    assert cursor.executemany.call_args[0][1] == [(1024, "A1"), (2371, "A2")]


def test_自增_id_轉成原生_python_int():
    """釘住不用 df.values.tolist() 的理由：numpy.int64 不是 int 的子類，pymysql 會拒收。"""
    _, cursor = _run()
    first_value = cursor.executemany.call_args[0][1][0][0]

    assert type(first_value) is int


def test_沒有資料列時不建立連線():
    """空 DataFrame 代表沒東西要回填，應記 warning 後直接返回，不開連線。"""
    empty = DF.iloc[0:0]

    with patch("src.util.mysql_utils.get_pymysql_conn_to_mysql") as fake_conn:
        update_table(
            empty,
            table="fact_accident_main",
            where_columns=["accident_id"],
            set_columns=["weather_record_id"],
        )

    fake_conn.assert_not_called()


def test_成功時提交且釋放資源():
    """正常路徑應 commit，並關閉 cursor 與 conn。"""
    conn, cursor = _run()

    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()
    cursor.close.assert_called_once()
    conn.close.assert_called_once()


def test_更新失敗時復原事務並原樣拋出():
    """依 ADR-0001，不轉換例外型別；且必須 rollback。"""
    conn, cursor = MagicMock(), MagicMock()
    conn.cursor.return_value = cursor
    cursor.executemany.side_effect = pymysql.MySQLError("lock wait timeout")

    with patch("src.util.mysql_utils.get_pymysql_conn_to_mysql", return_value=conn):
        with pytest.raises(pymysql.MySQLError, match="lock wait timeout"):
            update_table(
                DF,
                table="fact_accident_main",
                where_columns=["accident_id"],
                set_columns=["weather_record_id"],
            )

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once()


def test_where_columns_為空時拒絕執行():
    """Where 為空會組出無條件的 UPDATE，等於整張表被覆寫，必須擋在組 SQL 之前。"""
    with pytest.raises(ValueError, match="where_columns"):
        update_table(
            DF,
            table="fact_accident_main",
            where_columns=[],
            set_columns=["weather_record_id"],
        )


def test_set_columns_為空時拒絕執行():
    """Set 為空時 SQL 無法組成。"""
    with pytest.raises(ValueError, match="set_columns"):
        update_table(
            DF,
            table="fact_accident_main",
            where_columns=["accident_id"],
            set_columns=[],
        )


def test_缺少欄位時拋出_keyerror():
    """欄位缺漏應直指缺了哪一欄，而不是等 pymysql 回一個看不出原因的錯誤。"""
    with pytest.raises(KeyError, match="weather_record_id"):
        update_table(
            DF.loc[:, ["accident_id"]],
            table="fact_accident_main",
            where_columns=["accident_id"],
            set_columns=["weather_record_id"],
        )
