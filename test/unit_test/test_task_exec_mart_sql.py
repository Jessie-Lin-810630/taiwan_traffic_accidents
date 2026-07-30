"""驗證 ADR-0005：mart SQL 執行層的 finally 只釋放資源，不吞例外。"""

from unittest.mock import MagicMock, patch

import pymysql
import pytest

from src.task.exec_mart_sql import (
    exec_mart_sql_files,
    exec_sql_multistatement,
    find_sql_files,
)


def _fake_conn():
    """回傳 (conn, cursor)；next_result() 一律回 False 以結束消耗迴圈。"""
    conn, cursor = MagicMock(), MagicMock()
    conn.cursor.return_value = cursor
    conn.next_result.return_value = False
    return conn, cursor


def _patch_conn(conn):
    return patch(
        "src.task.exec_mart_sql.get_pymysql_conn_to_mysql_multistatement",
        return_value=conn,
    )


# --- find_sql_files ---------------------------------------------------------


def test_找不到_sql_檔案時拋出(tmp_path):
    """空目錄不該回傳空 list 讓下游安靜地什麼都不做。"""
    with pytest.raises(FileNotFoundError, match=".sql files not found"):
        find_sql_files(tmp_path)


def test_遞迴蒐集所有_sql_檔案(tmp_path):
    """子目錄下的 .sql 也要納入。"""
    (tmp_path / "a.sql").write_text("SELECT 1;")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.sql").write_text("SELECT 2;")
    (tmp_path / "c.txt").write_text("not sql")

    found = find_sql_files(str(tmp_path))

    assert sorted(p.split("/")[-1] for p in found) == ["a.sql", "b.sql"]


# --- exec_mart_sql_files ----------------------------------------------------


def test_執行失敗時復原事務並原樣拋出(tmp_path):
    """核心：例外必須穿透 finally，不再被 return 吞掉。"""
    sql_file = tmp_path / "mart.sql"
    sql_file.write_text("CREATE TABLE x (id INT);")
    conn, cursor = _fake_conn()
    cursor.execute.side_effect = pymysql.MySQLError("syntax error")

    with _patch_conn(conn):
        with pytest.raises(pymysql.MySQLError, match="syntax error"):
            exec_mart_sql_files([str(sql_file)], "traffic_accidents")

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    cursor.close.assert_called_once()
    conn.close.assert_called_once()


def test_關閉失敗不會取代正在傳播的例外(tmp_path):
    """Finally 內的 close() 若拋例外，原始的資料庫錯誤仍須是傳出去的那一個。"""
    sql_file = tmp_path / "mart.sql"
    sql_file.write_text("CREATE TABLE x (id INT);")
    conn, cursor = _fake_conn()
    cursor.execute.side_effect = pymysql.MySQLError("syntax error")
    cursor.close.side_effect = pymysql.MySQLError("cursor already closed")
    conn.close.side_effect = pymysql.MySQLError("connection already closed")

    with _patch_conn(conn):
        with pytest.raises(pymysql.MySQLError, match="syntax error"):
            exec_mart_sql_files([str(sql_file)], "traffic_accidents")


def test_連線建立失敗時不會變成_unboundlocalerror():
    """Conn 未綁定時，except 與 finally 都不得引用它而遮蔽原始錯誤。"""
    with patch(
        "src.task.exec_mart_sql.get_pymysql_conn_to_mysql_multistatement",
        side_effect=pymysql.OperationalError("can't connect"),
    ):
        with pytest.raises(pymysql.OperationalError, match="can't connect"):
            exec_mart_sql_files(["/nonexistent.sql"], "traffic_accidents")


def test_檔案讀取失敗時原樣拋出():
    """讀檔錯誤同樣不該被 finally 吞掉。"""
    conn, _ = _fake_conn()

    with _patch_conn(conn):
        with pytest.raises(OSError):
            exec_mart_sql_files(["/nonexistent/mart.sql"], "traffic_accidents")

    conn.rollback.assert_called_once()


def test_全數成功才提交一次(tmp_path):
    """多檔案共用一個事務，全部執行完才 commit。"""
    files = []
    for name in ("a.sql", "b.sql"):
        f = tmp_path / name
        f.write_text("SELECT 1;")
        files.append(str(f))
    conn, cursor = _fake_conn()

    with _patch_conn(conn):
        exec_mart_sql_files(files, "traffic_accidents")

    assert cursor.execute.call_count == 2
    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()
    cursor.close.assert_called_once()
    conn.close.assert_called_once()


def test_消耗掉每個檔案的所有_result_set(tmp_path):
    """Multistatement 的 result set 未消耗完會讓下一份檔案報錯。"""
    sql_file = tmp_path / "mart.sql"
    sql_file.write_text("SELECT 1; SELECT 2;")
    conn, _ = _fake_conn()
    conn.next_result.side_effect = [True, True, False]

    with _patch_conn(conn):
        exec_mart_sql_files([str(sql_file)], "traffic_accidents")

    assert conn.next_result.call_count == 3


def test_未指定資料庫時取用環境變數(tmp_path, monkeypatch):
    """DAG 端不傳 database 時的預設行為。"""
    sql_file = tmp_path / "mart.sql"
    sql_file.write_text("SELECT 1;")
    monkeypatch.setenv("MYSQL_DATABASE", "traffic_accidents")
    conn, _ = _fake_conn()

    with _patch_conn(conn) as fake_connect:
        exec_mart_sql_files([str(sql_file)])

    fake_connect.assert_called_once_with("traffic_accidents")


# --- exec_sql_multistatement ------------------------------------------------


def test_單段_sql_失敗時復原並原樣拋出():
    """零呼叫端但保留的路徑，同樣不得吞例外（ADR-0005 子決策 2）。"""
    conn, cursor = _fake_conn()
    cursor.execute.side_effect = pymysql.MySQLError("syntax error")

    with _patch_conn(conn):
        with pytest.raises(pymysql.MySQLError, match="syntax error"):
            exec_sql_multistatement("CREATE TABLE x (id INT);", "traffic_accidents")

    conn.rollback.assert_called_once()
    cursor.close.assert_called_once()
    conn.close.assert_called_once()


def test_單段_sql_連線失敗時不會變成_unboundlocalerror():
    """原實作把 conn 綁定在 try 內卻於 except/finally 引用，會遮蔽原始錯誤。"""
    with patch(
        "src.task.exec_mart_sql.get_pymysql_conn_to_mysql_multistatement",
        side_effect=pymysql.OperationalError("can't connect"),
    ):
        with pytest.raises(pymysql.OperationalError, match="can't connect"):
            exec_sql_multistatement("SELECT 1;", "traffic_accidents")


def test_單段_sql_成功時提交並釋放資源():
    """正常路徑要 commit，且 cursor 與 conn 都關閉。"""
    conn, cursor = _fake_conn()

    with _patch_conn(conn):
        exec_sql_multistatement("SELECT 1;", "traffic_accidents")

    conn.commit.assert_called_once()
    cursor.close.assert_called_once()
    conn.close.assert_called_once()
