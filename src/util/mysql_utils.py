"""MySQL 的唯一存取面：連線取得、Schema檢查、查詢與 upsert 寫入資料表。

連線分兩種，有各自適用情境：

- SQLAlchemy Engine 製作連線池，供 DQL 與 DDL 使用，
且搭配資料庫名稱與 Engine 物件組成的全域變數，作為快取在行程內，
防止對同一資料庫重複建立連線池。

- pymysql 裸連線，專供批次 upsert 與 multistatement 使用。

不論使用哪一種連線方式，一律需透過本模組的函式，且不要自行 `create_engine()`，
也不要對取回的 Engine 呼叫 `dispose()`（那等於丟棄整個連線池）。

連線設定取自環境變數:

- 有預設值: `MYSQL_HOST`（localhost）、`MYSQL_PORT`（3306）
- 必填: `MYSQL_USER`、`MYSQL_PASSWORD`，缺少時在建立連線的那一刻拋出

Notes:
    Engine 快取策略參考 ADR-0004，必填環境變數的檢查時機參考 ADR-0008。
"""

import os

import pandas as pd
import pymysql
from dotenv import load_dotenv
from pymysql import Connection
from pymysql.constants import CLIENT
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.util.logger_crtx import get_logger

logger = get_logger(__name__)

load_dotenv()
host = os.getenv("MYSQL_HOST", "localhost")
port = os.getenv("MYSQL_PORT", 3306)
username = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")


def _require_credentials() -> None:
    """建立連線前確認必填的帳號密碼環境變數存在。

    先檢查再連線，錯誤訊息才會直指缺少的環境變數；若放任 `None` 往下走，
    連線字串會變成 `mysql+pymysql://None:None@...`，MySQL 只會回覆認證失敗，
    看不出真正的原因。

    Raises:
        ValueError: `MYSQL_USER` 或 `MYSQL_PASSWORD` 未設定。

    Notes:
        參考 ADR-0008。
    """
    if not username:
        raise ValueError("未設定 MYSQL_USER，請檢查環境變數設置")
    if not password:
        raise ValueError("未設定 MYSQL_PASSWORD，請檢查環境變數設置")


def close_quietly(resource, resource_name: str) -> None:
    """關閉資源，並確保關閉失敗不會取代正在傳播的例外。

    供 `finally` 區塊呼叫。`close()` 自身可能拋出例外（例如 pymysql 在連線
    已斷開時），裸呼叫會讓這個次要錯誤取代 `except` 剛拋出的原始錯誤，
    因此這裡把關閉失敗降級為 warning 並吞下。

    Args:
        resource: 任何具備 `close()` 的資源，例如 `Connection` 物件；
        若傳入 `None` 時直接略過。
        resource_name (str): 記錄於 warning 訊息中的資源名稱。

    Notes:
        參考 ADR-0005。
    """
    if resource is None:
        return
    try:
        resource.close()
    except Exception as close_err:
        logger.warning(f"Failed to close {resource_name}: {close_err}")


# 以資料庫名為 key 的模組層全域變數， value 為 Engine 物件，
# 作為 Engine 的快取功用，避免外部函式調用時浪費快取、建立過多新的連線池。
# 連線池設定額外透過 _create_engine()。
_ENGINES: dict[str | None, Engine] = {}


def _create_engine(database: str | None = None) -> Engine:
    """建立一個連往 MySQL 的 SQLAlchemy Engine。

    僅供 `get_engine_to_mysql()` 在 `_ENGINES` 中還沒有該資料庫的 Engine 時呼叫；
    外部請一律使用 `get_engine_to_mysql()`，以免繞過快取而重複建立連線池。

    Args:
        database (str | None): 要連往的資料庫名稱；`None` 代表不指定資料庫。

    Returns:
        Engine: 連往指定資料庫的 SQLAlchemy Engine，連線池大小 5、
            每小時回收連線、取用前先探活。

    Raises:
        ValueError: `MYSQL_USER` 或 `MYSQL_PASSWORD` 未設定。
    """
    _require_credentials()

    if database:
        connection_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    else:
        connection_url = (
            f"mysql+pymysql://{username}:{password}@{host}:{port}/?charset=utf8mb4"
        )
    engine = create_engine(
        connection_url,
        echo=False,
        pool_size=5,
        pool_recycle=3600,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 120},
    )
    return engine


def get_engine_to_mysql(database: str | None = None) -> Engine:
    """取得連往指定資料庫的 SQLAlchemy Engine，同一資料庫在行程內共用同一個。

    只有 `_ENGINES` 中還沒有該資料庫的 Engine 時才會真的建立，並留下一行 info 日誌。
    連線池的生命週期與行程等長，因此不需要（也不應該）由呼叫端 `dispose()`，行程的詮釋見下方：

    根據本專案的兩個執行環境，有兩種詮釋：

    - Airflow 容器：本專案使用 LocalExecutor，一個 task 就是一個行程。同一條 DAG 的每個 task 各有
      一份自己的 `_ENGINES`，task 結束行程就消滅，連線池亦跟著回收。共用只發生在單一 task 內部的多次呼叫。
    - Streamlit 容器：一個容器實例就是一個常駐行程，Engine 會被所有分頁與所有
      使用者的 session 共用，切換分頁不會重建，只有容器重啟或水平擴充時，才會重建連線池。

    Args:
        database (str | None): 要連往的資料庫名稱；`None` 代表不指定資料庫
            （例如要建立資料庫本身時）。

    Returns:
        Engine: 連往指定資料庫的 SQLAlchemy Engine。

    Raises:
        ValueError: 首次建立時 `MYSQL_USER` 或 `MYSQL_PASSWORD` 未設定。

    Notes:
        參考 ADR-0004。
    """
    if database not in _ENGINES:
        logger.info(f"==== Creating SQLAlchemy Engine for database `{database}` ====")
        _ENGINES[database] = _create_engine(database)
    return _ENGINES[database]


def get_table_from_sqlserver(
    dql_str: str, params: dict | None = None, *, database: str | None = None
) -> pd.DataFrame:
    """執行 SELECT 查詢並把結果包成 DataFrame 回傳。

    全專案查詢 MySQL 的統一入口。

    Args:
        dql_str (str): 要執行的 SELECT 敘述，named placeholder 必須寫成 `:name`。
        params (dict | None): 對應 named placeholder 的參數；一律用它傳值，
            請不要把值直接內插進 SQL 字串。
        database (str | None): 資料表所在的資料庫名稱。

    Returns:
        pandas.DataFrame: 查詢結果；查無資料時為帶欄位名的空 DataFrame。

    Raises:
        SQLAlchemyError: 連線失敗或 SQL 執行失敗，原樣往外拋。

    Examples:
        以下查詢：

            get_table_from_sqlserver(
                "SELECT city, COUNT(*) AS cnt FROM fact_accident_main "
                "WHERE year = :year GROUP BY city",
                params={"year": 113},
                database="traffic_accident",
            )

        回傳的 DataFrame 形如：

            city    cnt
            臺北市  12034
            新北市  18876
    """
    engine = get_engine_to_mysql(database)

    with engine.connect() as conn:
        result = conn.execute(text(str(dql_str)), parameters=params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df


def get_pymysql_conn_to_mysql(database: str | None) -> Connection:
    """建立一條連往 MySQL 的 pymysql 裸連線，供批次寫入使用。

    因批次 upsert 走 `cursor.executemany()` 比 `DataFrame.to_sql()` 快得多，
    因此寫入路徑用此連線而非 Engine。
    注意必須手動 `commit` 或 `rollback`，並請在 `finally` 中以 `close_quietly()` 收尾。

    Args:
        database (str | None): 要連往的資料庫名稱。

    Returns:
        Connection: 已連上指定資料庫的 pymysql 連線，`autocommit` 為關閉，

    Raises:
        ValueError: `MYSQL_USER` 或 `MYSQL_PASSWORD` 未設定。
        pymysql.MySQLError: 連線建立失敗（主機不可達、認證失敗、逾時）。
    """
    _require_credentials()

    conn = pymysql.connect(
        host=host,
        port=int(port),
        user=username,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=60,
        read_timeout=600,
        write_timeout=600,
    )
    return conn


def get_pymysql_conn_to_mysql_multistatement(database: str | None) -> Connection:
    """建立一條允許一次送出多句 SQL 的 pymysql 裸連線。

    與 `get_pymysql_conn_to_mysql()` 的差別只在多帶 `CLIENT.MULTI_STATEMENTS`，
    讓一個 `execute()` 能執行以分號分隔的多句敘述。供 mart 層 SQL 腳本使用，
    那些腳本是整份檔案一次執行的。
    注意必須手動 `commit` 或 `rollback`，並請在 `finally` 中以 `close_quietly()` 收尾。

    Args:
        database (str | None): 要連往的資料庫名稱。

    Returns:
        Connection: 已連上指定資料庫、允許 multistatement 的 pymysql 連線，
            `autocommit` 為關閉。

    Raises:
        ValueError: `MYSQL_USER` 或 `MYSQL_PASSWORD` 未設定。
        pymysql.MySQLError: 連線建立失敗（主機不可達、認證失敗、逾時）。
    """
    _require_credentials()

    conn = pymysql.connect(
        host=host,
        port=int(port),
        user=username,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=60,
        read_timeout=600,
        write_timeout=600,
        client_flag=CLIENT.MULTI_STATEMENTS,
    )
    return conn


def inspect_table_exists(conn, table_name: str) -> bool:
    """檢查資料表是否已存在於連線所指的資料庫中。

    Args:
        conn: 已指定資料庫的 SQLAlchemy 連線。
        table_name (str): 要檢查的資料表名稱。

    Returns:
        bool: 資料表已存在為 `True`，不存在為 `False`。
    """
    logger.info(f"Checking Table existence {table_name}.....")
    inspector = inspect(conn)
    # 相當於執行 SQL 查詢:
    # select "target_table_name" in
    #       (select table_name
    #           from information_schema.tables
    # 	            where table_schema = "你的資料庫名稱");

    return table_name in inspector.get_table_names()


def inspect_table(engine: Engine, db_name: str, table_name: str) -> None:
    """把資料表的 Schema、總筆數與前三列資料寫進日誌，供人工檢視。

    只作為排查與驗收用途，不回傳任何值，所有結果都以 info 日誌輸出。

    Args:
        engine (Engine): 已指定資料庫的 SQLAlchemy Engine。
        db_name (str): 資料表所在的資料庫名稱。
        table_name (str): 要檢視的資料表名稱。

    Raises:
        Exception: 查詢過程中的任何錯誤，記下 error 後原樣往外拋
            （常見情況是資料表不存在）。
    """
    full_table_path = f"`{db_name}`.`{table_name}`"
    logger.info(f"Checking Table: {full_table_path}.....")

    def _extracted_from_inspect_table(
        full_table_path: str, conn: Connection
    ) -> str | pd.DataFrame:
        """依序記錄 Schema 與筆數，並回傳前三列資料。

        Args:
            full_table_path (str): 以反引號括起的完整資料表路徑。
                例如: `資料庫名`.`資料表名稱`
            conn (Connection): 已開啟的 SQLAlchemy 連線。

        Returns:
            str | pandas.DataFrame: 資料表有資料時回傳前三列的 DataFrame，
                形如：

                    accident_id  city    deaths
                    1130101001   臺北市  0
                    1130101002   新北市  1

                資料表為空時回傳說明字串 " This table is currently empty."。
        """
        # 1. 型態與索引檢查
        logger.info("[1. Schema Definition]")
        schema = pd.read_sql(text(f"DESC {full_table_path}"), conn)
        logger.info(f"\n{schema[['Field', 'Type', 'Key']]}")

        # 2. 筆數統計
        count = conn.execute(text(f"SELECT COUNT(*) FROM {full_table_path}")).scalar()
        logger.info(f"[2. Total Rows: {count:,}]")

        # 3. 資料預覽 (limit 3)
        data = pd.read_sql(text(f"SELECT * FROM {full_table_path} LIMIT 3"), conn)
        logger.info("[3. Data Preview]:")

        # 4. 檢視回傳結果
        if data.empty:
            return " This table is currently empty."
        else:
            return data

    try:
        with engine.connect() as conn:
            result = _extracted_from_inspect_table(full_table_path, conn)
            logger.info(f"{result}")
    except Exception:
        logger.error(f"Error inspecting {full_table_path}")
        raise


def create_database(engine: Engine, database_name: str) -> None:
    """建立資料庫，已存在時不做任何事。

    以 `CREATE DATABASE IF NOT EXISTS` 執行，字元集固定為 utf8mb4；
    連線以 AUTOCOMMIT 隔離級別開啟，避免建立資料庫的語句被包在事務中。

    Args:
        engine (Engine): 未指定資料庫的 SQLAlchemy Engine。
        database_name (str): 要建立的資料庫名稱。

    Raises:
        SQLAlchemyError: 建立失敗（權限不足、連線中斷等），記下 error 後原樣往外拋。
        Exception: 其他非預期錯誤，同樣記下 error 後原樣往外拋。
    """
    logger.info(f"==== Checking/Creating database: '{database_name}' ====")

    try:
        # 使用 AUTOCOMMIT 隔離級別建立連線，防止部分資料庫因事務鎖定而無法建立 database。
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            # 執行建立資料庫語句
            conn.execute(
                text(
                    f"CREATE DATABASE IF NOT EXISTS `{database_name}` CHARACTER SET utf8mb4;"
                )
            )
            logger.info(f"Database '{database_name}' checked/created successfully.")

    except SQLAlchemyError:
        logger.error(
            f"SQLAlchemy database error occurred while creating '{database_name}'."
        )
        raise

    except Exception:
        logger.error(
            f"Unexpected error occurred while creating '{database_name}'.",
        )
        raise


def create_tables(engine: Engine, tables: dict[str, str]) -> None:
    """依傳入的 DDL 逐一建立資料表，已存在的資料表會被跳過，含 commit、rollback 與連線關閉。

    每張表都先用 `inspect_table_exists()` 檢查再決定是否執行 DDL，
    目的是讓日誌寫入時能區分「已存在、略過」與「本次建立」。

    Args:
        engine (Engine): 已指定資料庫的 SQLAlchemy Engine。
        tables (dict[str, str]): 資料表名稱對應其 `CREATE TABLE` 敘述。
            鍵必須與 DDL 實際建立的資料表同名，存在性檢查才會正確。

    Raises:
        SQLAlchemyError: DDL 執行失敗，事務已由 `engine.begin()` 自動復原後往外拋。
        Exception: 其他非預期錯誤，同樣記下 error 後原樣往外拋。
    """
    logger.info("==== Starting creation of tables... ====")

    try:
        # engine.begin() 會在離開 context 時自動提交，失敗則自動 rollback。
        with engine.begin() as conn:
            for table_name, ddl in tables.items():
                # 檢查 table 是否存在，若已經存在則 logger 紀錄已存在且跳過重複建立。
                if inspect_table_exists(conn, table_name):
                    logger.info(f"Table '{table_name}' already exists, skipping.")
                    continue
                logger.info(f"Creating table '{table_name}'...")
                conn.execute(text(ddl))
                logger.info(f"Table '{table_name}' created successfully.")

    except SQLAlchemyError:
        logger.error("SQLAlchemy error occurred during table creation.")
        raise

    except Exception:
        logger.error("Unexpected error occurred during table creation.")
        raise


def upsert_to_table(
    df: pd.DataFrame,
    table: str,
    update_columns: list[str],
    database: str | None = None,
) -> None:
    """把 DataFrame 整批寫入 MySQL 資料表，主鍵重複時改為更新，含 commit、rollback 與連線關閉。

    全專案寫入 MySQL 的統一入口，`l_*` 系列 task 都經由它落地。SQL 以
    `INSERT ... ON DUPLICATE KEY UPDATE` 組成、走 `executemany()` 一次送出，
    因此同一批資料重跑不會產生重複列。寫入失敗會先復原事務再原樣往外拋，
    讓 traceback 完整保留給呼叫端（Airflow task 日誌或地端 stderr）。

    Args:
        df (pandas.DataFrame): 待寫入的資料，欄位名須與目標資料表一致，
            欄位順序即 INSERT 的欄位順序。
        table (str): 目標資料表名稱。
        update_columns (list[str]): 主鍵衝突時要更新的欄位名，會被組成
            `col=VALUES(col)` 片段；不可為空。
        database (str | None): 資料表所在的資料庫名稱。

    Raises:
        ValueError: `update_columns` 為空，SQL 無法組成。
        pymysql.MySQLError: 寫入失敗，事務復原後原樣往外拋。
        Exception: 其他非預期錯誤，同樣復原事務後往外拋。
    """
    if not update_columns:
        raise ValueError(f"upsert 至 `{table}` 需要至少一個 update_columns 欄位")

    # 1. 準備INSERT資料表時需要的SQL語句，採用UPSERT
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    update_part = ", ".join(f"{col}=VALUES({col})" for col in update_columns)

    dml_str = f"""INSERT INTO {table} ({columns})
                  VALUES ({placeholders})
                  ON DUPLICATE KEY UPDATE {update_part};
                """
    # MySQL 8.0.20 以後 VALUES() 已標記為 deprecated，可改寫成：
    #     INSERT INTO {table} ({columns}) VALUES ({placeholders}) AS n
    #     ON DUPLICATE KEY UPDATE {col}=n.{col}

    logger.info(f"==== Start to insert into table `{table}` by upserting. ====")

    conn = None
    cursor = None

    try:
        # 2. 建立連線與游標
        conn = get_pymysql_conn_to_mysql(database)
        cursor = conn.cursor()

        # 3. 執行批次寫入與提交
        cursor.executemany(
            dml_str, df.values.tolist()
        )  # df.values.tolist() 轉回 list of lists
        conn.commit()

    except pymysql.MySQLError:
        # 4. 資料庫例外處理：復原事務，並重新拋出原始錯誤
        logger.error(f"Database error while inserting into `{table}`.")
        if conn:
            conn.rollback()
            logger.info("Transaction rollbacked successfully.")
        raise

    except Exception:
        logger.error(f"Unexpected error while inserting into `{table}`.")
        if conn:
            conn.rollback()
        raise

    else:
        logger.info(f"==== Successfully inserted into table `{table}` ====")

    finally:
        close_quietly(cursor, "cursor")
        close_quietly(conn, "connection")

    return None


def update_table(
    df: pd.DataFrame,
    table: str,
    where_columns: list[str],
    set_columns: list[str],
    database: str | None = None,
) -> None:
    """把 DataFrame 整批更新回既有資料列，含 commit、rollback 與連線關閉。

    此函式將 SQL 組成 `UPDATE ... SET ... WHERE ...` ，以 `executemany()` 一次送出，
    寫入失敗會先復原事務再樣往 raise 例外。

    Args:
        df (pandas.DataFrame): 待更新的資料，須同時含 `set_columns` 與
            `where_columns` 的所有欄位；欄位順序不影響結果，內部會重排。
        table (str): 目標資料表名稱。
        where_columns (list[str]): 用來定位資料列的欄位名，會被組成
            `col=%s AND ...`；不可為空，否則會更新整張表。
        set_columns (list[str]): 要被更新的欄位名，會被組成 `col=%s`；不可為空。
        database (str | None): 資料表所在的資料庫名稱。

    Raises:
        ValueError: `set_columns` 或 `where_columns` 為空，SQL 無法安全組成。
        KeyError: `df` 缺少 `set_columns` 或 `where_columns` 中的欄位。
        pymysql.MySQLError: 更新失敗，事務復原後原樣往外拋。
        Exception: 其他非預期錯誤，同樣復原事務後往外拋。
    """
    if not set_columns or not where_columns:
        raise ValueError(
            f"update 至 `{table}` 需要至少一個 set_columns 與 where_columns 欄位"
        )

    missing = [c for c in set_columns + where_columns if c not in df.columns]
    if missing:
        raise KeyError(f"update 至 `{table}` 缺少欄位：{missing}")

    # 1. 準備 UPDATE 資料表時需要的 SQL 語句
    set_part = ", ".join(f"{col}=%s" for col in set_columns)  # col1=%s, col2=%s
    where_part = " AND ".join(
        f"{col}=%s" for col in where_columns
    )  # col3=%s AND col4=%s

    dml_str = f"""UPDATE {table} SET {set_part}
                WHERE ({where_part});
                """
    # 佔位符的順序是先 SET 後 WHERE，欄位順序必須跟著對齊
    df = df.loc[:, set_columns + where_columns]

    # 這裡刻意不用 upsert_to_table 的 `df.values.tolist()`：本函式常帶著資料庫
    # 自增而來的 int64 欄位，`numpy.int64` 不是 `int` 的子類，pymysql 會拒收；
    # `to_records().tolist()` 會轉成原生 Python 型別。
    data = df.to_records(index=False).tolist()

    if not data:
        logger.warning(f"沒有任何資料列要更新到 `{table}`，略過")
        return None

    logger.info(f"==== Start to update table `{table}`. ====")

    conn = None
    cursor = None

    try:
        # 2. 建立連線與游標
        conn = get_pymysql_conn_to_mysql(database)
        cursor = conn.cursor()

        # 3. 執行批次寫入與提交
        cursor.executemany(dml_str, data)
        conn.commit()

    except pymysql.MySQLError:
        # 4. 資料庫例外處理：復原事務，並重新拋出原始錯誤
        logger.error(f"Database error while updating `{table}`.")
        if conn:
            conn.rollback()
            logger.info("Transaction rollbacked successfully.")
        raise

    except Exception:
        logger.error(f"Unexpected error while updating `{table}`.")
        if conn:
            conn.rollback()
        raise

    else:
        logger.info(f"==== Successfully updated table `{table}` ====")

    finally:
        close_quietly(cursor, "cursor")
        close_quietly(conn, "connection")

    return None
