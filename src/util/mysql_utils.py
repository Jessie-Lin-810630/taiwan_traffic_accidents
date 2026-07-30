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


def create_engine_to_mysql(database: str | None = None) -> Engine:
    """Create a SQLAlchemy engine to connect to a MySQL database.

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
        logger.error(f"Error inspecting {full_table_path}", exc_info=True)
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
        # exc_info=True 會將完整的資料庫 Traceback 寫入 Airflow /logs/
        logger.error(
            f"SQLAlchemy database error occurred while creating '{database_name}'.",
            exc_info=True,
        )
        raise

    except Exception:
        logger.error(
            f"Unexpected error occurred while creating '{database_name}'.",
            exc_info=True,
        )
        raise

    finally:
        engine.dispose()


def create_tables(engine: Engine) -> None:
    """Create tables in a designated MySQL database if the tables not exist.

    Parameters:
        engine (Engine): SQLAlchemy Engine instance connected to MySQL server (with specifying database)
    """
    logger.info("==== Starting creation of tables... ====")

    tables_to_create = {
        "dim_evnet_day": """CREATE TABLE IF NOT EXISTS `dim_event_day` (
                                            `day_id` INT AUTO_INCREMENT PRIMARY KEY NOT NULL COMMENT '日編號ID',
                                            `date` DATE COMMENT '事件日期',
                                            `weekday` VARCHAR(10) COMMENT '事件發生星期',
                                            `is_holiday` TINYINT COMMENT '是否放假',
                                            `national_activity` VARCHAR(20) COMMENT '是否有全國性活動，例如：總統上任、公投日、國定假日',
                                            CONSTRAINT `uk_dim_event_date` UNIQUE (`date`)
                                            ) charset=utf8mb4 COMMENT '事件日維度表';
                       """,
        "fact_accident_main": """CREATE TABLE IF NOT EXISTS `fact_accident_main` (
                                            `accident_id` VARCHAR(16) PRIMARY KEY NOT NULL COMMENT '車禍案件編號',
                                            `accident_type_id` BIGINT NOT NULL COMMENT '事故類別編號ID',
                                            `day_id` INT NOT NULL COMMENT '日編號ID',
                                            `accident_time` time COMMENT '車禍時段(HH:MM:SS)',
                                            `death_count` INT COMMENT '死亡人數',
                                            `injury_count` INT COMMENT '受傷人數',
                                            `longitude` decimal(10,6) COMMENT '經度',
                                            `latitude` decimal(10,6) COMMENT '緯度',
                                            CONSTRAINT `fk_fact_accmain_dayid` FOREIGN KEY (`day_id`)
                                                REFERENCES `dim_event_day`(`day_id`),
                                            UNIQUE KEY `uk_fact_accmain_daytimelonlat` (`day_id`, `accident_time`,
                                                                                        `longitude`,`latitude`),
                                            INDEX `idx_fact_accmain_lon` (`longitude`),
                                            INDEX `idx_fact_accmain_lat` (`latitude`)
                                            ) CHARSET=utf8mb4 COMMENT='車禍案件事實表';
                        """,
    }

    try:
        # engine.begin() 會在離開 context 時自動提交，失敗則自動 rollback。
        with engine.begin() as conn:
            for table_name, ddl in tables_to_create.items():
                # 檢查 table 是否存在，若已經存在則 logger 紀錄已存在且跳過重複建立。
                if inspect_table_exists(conn, table_name):
                    logger.info(f"Table '{table_name}' already exists, skipping.")
                    continue
                logger.info(f"Creating table '{table_name}'...")
                conn.execute(text(ddl))
                logger.info(f"Table '{table_name}' created successfully.")

    except SQLAlchemyError:
        # exc_info=True 會將完整的資料庫 Traceback 寫入 Airflow /logs/
        logger.error("SQLAlchemy error occurred during table creation.", exc_info=True)
        raise

    except Exception:
        logger.error("Unexpected error occurred during table creation.", exc_info=True)
        raise

    finally:
        engine.dispose()


def upsert_to_table(df: pd.DataFrame, database: str | None = None) -> None:
    """Write DataFrame records into a MySQL table using UPSERT (ON DUPLICATE KEY UPDATE).

    Provides a transaction rollback mechanism and re-raises the original
    database error so that the traceback is preserved for the caller
    (Airflow task log or local stderr alike).

    Parameters:
        df (pandas.DataFrame): The DataFrame containing the records to be inserted/updated.
        database (str): name of database where the table locates.
    """
    # 1. 準備INSERT資料表時需要的SQL語句，採用UPSERT
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    # update_part = "<更新的欄位名1>=VALUES(<更新的欄位名1>), <更新的欄位名2>=VALUES(<更新的欄位名2>)"
    update_part = "accident_weekday=VALUES(accident_weekday)"

    dml_str = f"""INSERT INTO dim_accident_day ({columns})
                  VALUES ({placeholders})
                  ON DUPLICATE KEY UPDATE {update_part};
                """
    # MySQL 8.0.20 以後可寫成
    # update_part = "accident_weekday=n.accident_weekday"
    # dml_str = f"""INSERT INTO dim_accident_day ({columns})
    #               VALUES ({placeholders}) AS n
    #               ON DUPLICATE KEY UPDATE {update_part};
    #             """

    logger.info("==== Starting insertion into table `dim_accident_day` ====")

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
        logger.error("Database error occurred during insertion.", exc_info=True)
        if conn:
            conn.rollback()
            logger.info("Transaction rollbacked successfully.")
        raise

    except Exception:
        logger.error("Unexpected error occurred during insertion.", exc_info=True)
        if conn:
            conn.rollback()
        raise

    else:
        logger.info("==== Successfully inserted into table `dim_accident_day` ====")

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
