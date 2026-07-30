"""MySQL 共用工具：SQLAlchemy Engine／pymysql 連線、綱要檢查與 upsert 寫入。"""

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


# 以資料庫名為鍵的模組層 Engine 快取（ADR-0004）：
# 每個 Engine 攜帶一個連線池，若每次呼叫都新建，池永遠不會服務第二個請求，
# pool_size / pool_recycle / pool_pre_ping 三個設定形同虛設。
_ENGINES: dict[str | None, Engine] = {}


def _create_engine(database: str | None = None) -> Engine:
    """建立一個連往 MySQL 的 SQLAlchemy Engine。

    僅供 `get_engine_to_mysql()` 在快取未命中時呼叫。
    外部請一律使用 `get_engine_to_mysql()`，以免繞過快取。

    Parameters:
        database (str | None): The name of the database to connect to.

    Returns:
        Engine: A SQLAlchemy Engine instance connected to the specified MySQL database.
    """
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

    Engine 內含連線池，生命週期與行程等長，因此不需要（也不應該）由呼叫端
    `dispose()`。未命中快取時才會真的建立，日誌因此可直接證明池只建立一次。

    Parameters:
        database (str | None): The name of the database to connect to.
            `None` 代表不指定資料庫（例如建立資料庫本身時）。

    Returns:
        Engine: A SQLAlchemy Engine instance connected to the specified MySQL database.
    """
    if database not in _ENGINES:
        logger.info(f"==== Creating SQLAlchemy Engine for database `{database}` ====")
        _ENGINES[database] = _create_engine(database)
    return _ENGINES[database]


def get_table_from_sqlserver(
    dql_str: str, params: dict | None = None, *, database: str | None = None
) -> pd.DataFrame:
    """以 DQL 查詢 MySQL 資料表並回傳 DataFrame。

    Parameters:
        dql_str (str): 要執行的 DQL（SELECT）敘述。
        params (dict | None): 綁定至敘述中具名佔位符的參數。
        database (str | None): 資料表所在的資料庫名稱。

    Returns:
        pandas.DataFrame: 查詢結果；欄位名取自查詢回傳的欄位。
    """
    engine = get_engine_to_mysql(database)

    with engine.connect() as conn:
        result = conn.execute(text(str(dql_str)), parameters=params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df


def get_pymysql_conn_to_mysql(database: str | None) -> Connection:
    """Create a pymysql Connection to connect to a MySQL database.

    More suitable for Upserting than using Pandas.to_sql().

    Parameters:
        database (str | None): The name of the database to connect to.

    Returns:
        Connection: A pymysql Connection instance connected to the specified MySQL database.
    """
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
    """Create a pymysql Connection to connect to a MySQL database.

    More suitable for Upserting than using Pandas.to_sql().

    Parameters:
        database (str | None): The name of the database to connect to.

    Returns:
        Connection: A pymysql Connection instance connected to the specified MySQL database.
    """
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
    """Inspect if the specified table_name already exists in the connected database.

    Parameters:
        conn (Connection): A SQLAlchemy Connection instance connected with MySQL server (with specified database)
        table_name (str): The name of table to be inspected whether exists or not.

    Returns:
        bool: A boolean value indicating if the table exists. True if exists, false if not exists.
    """
    logger.info(f"Checking Table existence {table_name}.....")
    inspector = inspect(conn)
    # 或是執行 SQL 查詢:
    # select table_name
    # from information_schema.tables
    # 	where table_schema = "你的資料庫名稱";

    return table_name in inspector.get_table_names()


def inspect_table(engine: Engine, db_name: str, table_name: str) -> None:
    """Inspect the schema, total row counts and preview the first 3 data rows.

    Parameters:
        engine (Engine): SQLAlchemy Engine instance connected to MySQL server (with specifying database)
        db_name (str): The name of the database where the table is located.
        table_name (str): The name of the table to inspect.
    """
    full_table_path = f"`{db_name}`.`{table_name}`"
    logger.info(f"Checking Table: {full_table_path}.....")

    def _extracted_from_inspect_table(
        full_table_path: str, conn: Connection
    ) -> str | pd.DataFrame:
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
    """Inspect if the designed MySQL database exists and create it if not exists.

    Parameters:
        engine (Engine): SQLAlchemy Engine instance connected to MySQL server (without specifying database)
        database_name (str): The name of the database to be created regardless of existing.
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
    """依傳入的 DDL 建立資料表；已存在的資料表會被跳過。

    以 `inspect_table_exists()` 先行檢查，因此日誌能精確區分
    「已存在、略過」與「本次建立」—— 這是 `CREATE TABLE IF NOT EXISTS`
    單獨使用時做不到的。

    Parameters:
        engine (Engine): 已指定資料庫的 SQLAlchemy Engine。
        tables (dict[str, str]): 資料表名稱對應其 `CREATE TABLE` 敘述。
            鍵必須與 DDL 實際建立的資料表同名，存在性檢查才會正確。

    Raises:
        SQLAlchemyError: DDL 執行失敗，事務已由 `engine.begin()` 自動復原。
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
    """以 UPSERT（INSERT ... ON DUPLICATE KEY UPDATE）將 DataFrame 寫入 MySQL 資料表。

    提供事務復原機制，並原樣拋出原始的資料庫錯誤，
    讓 traceback 完整保留給呼叫端（Airflow task log 或地端 stderr）。

    Parameters:
        df (pandas.DataFrame): 待寫入的資料；欄位名須與目標資料表一致。
        table (str): 目標資料表名稱。
        update_columns (list[str]): 主鍵衝突時要更新的欄位名，
            會被組成 `col=VALUES(col)` 片段。不可為空。
        database (str | None): 資料表所在的資料庫名稱。

    Raises:
        ValueError: `update_columns` 為空時，SQL 無法組成。
        pymysql.MySQLError: 寫入失敗，事務已復原後原樣拋出。
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

    logger.info(f"==== Starting insertion into table `{table}` ====")

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
        if cursor:
            try:
                cursor.close()
            except Exception as close_err:
                logger.warning(f"Failed to close cursor: {close_err}")
        if conn:
            try:
                conn.close()
            except Exception as close_err:
                logger.warning(f"Failed to close connection: {close_err}")

    return None
