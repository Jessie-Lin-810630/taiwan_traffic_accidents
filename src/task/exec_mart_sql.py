"""Mart 層 SQL 的蒐集與執行。

`src/task/mart_table_sql/*.sql` 是純 SQL 的 mart 層定義，每個檔案含多條敘述，
因此一律以 multistatement 連線執行。本模組由 `dags/d04_analysis_pedestrian_accidents`
包成 task 呼叫。
"""

import os
from pathlib import Path

from src.util.logger_crtx import get_logger
from src.util.mysql_utils import (
    close_quietly,
    get_pymysql_conn_to_mysql_multistatement,
)

logger = get_logger(__name__)


def find_sql_files(sql_files_dir: str | Path) -> list[str]:
    """遞迴蒐集目錄下所有 `.sql` 檔案的路徑。

    Parameters:
        sql_files_dir (str | Path): 要搜尋的目錄。

    Returns:
        list[str]: 找到的 `.sql` 檔案路徑。

    Raises:
        FileNotFoundError: 目錄下沒有任何 `.sql` 檔案。
    """
    if isinstance(sql_files_dir, str):
        sql_files_dir = Path(sql_files_dir)

    sql_file_paths = [str(f) for f in sql_files_dir.rglob("*.sql")]
    if not sql_file_paths:
        raise FileNotFoundError(
            f".sql files not found in the directory {sql_files_dir}!"
        )
    return sql_file_paths


def exec_sql_multistatement(sql_str: str, database: str | None) -> None:
    """以 multistatement 連線執行一段（可含多條敘述的）SQL。

    目前沒有呼叫端 —— `exec_mart_sql_files()` 直接逐檔執行。
    保留為單段 SQL 的執行路徑。

    Parameters:
        sql_str (str): 要執行的 SQL，可含多條以分號分隔的敘述。
        database (str | None): 目標資料庫名稱。

    Raises:
        pymysql.MySQLError: 執行失敗，事務已復原後原樣拋出。
    """
    conn = None
    cursor = None

    try:
        conn = get_pymysql_conn_to_mysql_multistatement(database)
        cursor = conn.cursor()
        cursor.execute(sql_str)

        # 連線為 autocommit=False，必須明確提交。
        conn.commit()

    except Exception:
        logger.error("SQL 執行失敗")
        if conn:
            conn.rollback()
            logger.info("Transaction rollbacked successfully.")
        raise

    else:
        logger.info("Mart 層資料表建立成功!")

    finally:
        close_quietly(cursor, "cursor")
        close_quietly(conn, "connection")


def exec_mart_sql_files(sql_file_paths: list[str], database: str | None = None) -> None:
    """依序讀取並執行多個 mart 層 SQL 檔案，全數成功後才提交。

    Parameters:
        sql_file_paths (list[str]): 要執行的 `.sql` 檔案路徑，依序執行。
        database (str | None): 目標資料庫名稱；未指定時取 `MYSQL_DATABASE`。

    Raises:
        pymysql.MySQLError: 任一檔案執行失敗，整批事務已復原後原樣拋出。
        OSError: 檔案讀取失敗。
    """
    if database is None:
        database = os.getenv("MYSQL_DATABASE")

    conn = None
    cursor = None
    file_path = None  # 供 except 區塊指出失敗的檔案，避免引用未綁定的迴圈變數

    try:
        conn = get_pymysql_conn_to_mysql_multistatement(database)
        cursor = conn.cursor()

        for i, file_path in enumerate(sql_file_paths):
            logger.info(f"正在處理第{i + 1}份: {os.path.basename(file_path)}")
            with open(file_path, mode="r") as f:
                sql_content = f.read()
            cursor.execute(sql_content)

            # 有可能資料庫還沒真正完成報錯，但 Python 認為已經跑完而提前印出「建立成功」。
            # 這裡強制消耗掉所有 result set，才能進入下一個檔案。
            while conn.next_result():
                pass
            logger.info("Mart 層資料表建立成功!")

        # 連線為 autocommit=False，必須明確提交。
        conn.commit()

    except Exception:
        logger.error(f"處理 sql file 失敗: {file_path}")
        if conn:
            conn.rollback()
            logger.info("Transaction rollbacked successfully.")
        raise

    else:
        logger.info("全數 sql file 解析且執行完成!")

    finally:
        close_quietly(cursor, "cursor")
        close_quietly(conn, "connection")
