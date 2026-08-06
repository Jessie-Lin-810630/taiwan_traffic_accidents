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
    """遞迴蒐集目錄底下所有 `.sql` 檔案的路徑。

    一份都找不到視為 FileNotFoundError。

    Args:
        sql_files_dir (str | Path): 要搜尋的目錄，會遞迴走訪子目錄。

    Returns:
        list[str]: 找到的 `.sql` 檔案路徑，形如：

            [
                "src/task/mart_table_sql/mart_pedestrian_accidents.sql",
                "src/task/mart_table_sql/mart_accident_hotspot.sql",
            ]

    Raises:
        FileNotFoundError: 目錄底下沒有任何 `.sql` 檔案。
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
    """以 multistatement 連線執行一段可含多條敘述的 SQL。

    整段 SQL 在同一個事務內執行，失敗即整段復原。目前沒有呼叫端，
    `exec_mart_sql_files()` 是直接逐檔執行的；本函式保留為單段 SQL 的執行路徑。

    Args:
        sql_str (str): 要執行的 SQL，可含多條以分號分隔的敘述。
        database (str | None): 目標資料庫名稱。

    Raises:
        pymysql.MySQLError: 連線或執行失敗，事務復原後原樣拋出。
        Exception: 其他非預期錯誤，同樣復原事務後往外拋。
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
    """依序讀取並執行多個 mart 層 `.sql` 檔案，全數成功後才一次提交。

    所有 `.sql` 檔案共用同一條連線與同一個 transaction，因此任一檔失敗會讓整批 rollback，
    不會留下只建到一半的 mart 層分析。

    Args:
        sql_file_paths (list[str]): 要執行的 `.sql` 檔案路徑，依清單順序執行。
        database (str | None): 目標資料庫名稱；未指定時取環境變數 `MYSQL_DATABASE`。

    Raises:
        pymysql.MySQLError: 任一檔案執行失敗，整批事務復原後原樣拋出。
        OSError: 檔案讀取失敗（路徑不存在或無權限）。
        Exception: 其他非預期錯誤，同樣復原事務後往外拋。
    """
    if database is None:
        database = os.getenv("MYSQL_DATABASE")

    conn = None
    cursor = None
    file_path = None

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

        # get_pymysql_conn_to_mysql_multistatement 採 autocommit=False。
        conn.commit()

    except Exception:
        logger.error(
            f"處理 sql file 失敗: {file_path}"
        )  # file_path 為 None，代表沒找到檔案
        if conn:
            conn.rollback()
            logger.info("Transaction rollbacked successfully.")
        raise

    else:
        logger.info("全數 sql file 解析且執行完成!")

    finally:
        close_quietly(cursor, "cursor")
        close_quietly(conn, "connection")
