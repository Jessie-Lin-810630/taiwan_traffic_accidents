from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pymysql
from pymysql import Connection
from pymysql.constants import CLIENT
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
import redis
import sys

if "/opt/airflow" in sys.path:
    from airflow.models import Variable
    from airflow.exceptions import AirflowException

load_dotenv()
host = os.getenv("MYSQL_HOST", "localhost")
port = os.getenv("MYSQL_PORT", 3306)
username = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")

redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = os.getenv("REDIS_PORT", 6379)
redis_password = os.getenv("REDIS_PASSWORD")


def create_engine_to_mysql(database: str | None = None) -> Engine:
    """
    Create a SQLAlchemy engine to connect to a MySQL database.

    Parameters:
        database (str | None): The name of the database to connect to.

    Returns:
        Engine: A SQLAlchemy Engine instance connected to the specified MySQL database.
    """

    # Create the connection string
    if database:
        connection_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    else:
        connection_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/?charset=utf8mb4"
    # Create and return the SQLAlchemy engine
    engine = create_engine(connection_url, echo=False,
                           pool_size=5, pool_recycle=3600, pool_pre_ping=True,
                           connect_args={"connect_timeout": 120})
    return engine


def get_pymysql_conn_to_mysql(database: str | None) -> Connection:
    """Create a pymysql Connection to connect to a MySQL database. More suitable for Upserting
    than using Pandas.to_sql().
    Parameters:
        database (str | None): The name of the database to connect to.

    Returns:
        Connection: A pymysql Connection instance connected to the specified MySQL database."""
    conn = pymysql.connect(host=host,
                           port=int(port),
                           user=username,
                           password=password,
                           database=database,
                           charset="utf8mb4",
                           autocommit=False,
                           connect_timeout=60,      # 連線建立超時
                           read_timeout=600,        # 讀取超時（適合大查詢）
                           write_timeout=600,       # 寫入超時
                           )
    return conn


def get_pymysql_conn_to_mysql_multistatement(database: str | None) -> Connection:
    """Create a pymysql Connection to connect to a MySQL database. More suitable for Upserting
    than using Pandas.to_sql().
    Parameters:
        database (str | None): The name of the database to connect to.

    Returns:
        Connection: A pymysql Connection instance connected to the specified MySQL database."""
    conn = pymysql.connect(host=host,
                           port=int(port),
                           user=username,
                           password=password,
                           database=database,
                           charset="utf8mb4",
                           autocommit=False,
                           connect_timeout=60,      # 連線建立超時
                           read_timeout=600,        # 讀取超時（適合大查詢）
                           write_timeout=600,       # 寫入超時
                           client_flag=CLIENT.MULTI_STATEMENTS)  # 整段腳本寫入
    return conn


def create_database(engine: Engine, database_name: str) -> None:
    """Inspect if the designed database exists and create it if not exists."""
    try:
        with engine.connect() as conn:
            conn.execute(text(
                f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET utf8mb4;"))
            print(f"Database '{database_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred while creating the database: {e}")
        if "/opt/airflow" in sys.path:
            raise AirflowException
        raise Exception
    finally:
        engine.dispose()
    return None


def create_redis_client(decode_response: bool = False):
    # decode_responses=False 是關鍵設定。因為要存入 Pickle 二進位資料(DataFrame)
    # 如果設為 True，Redis 會強制轉為字串，導致二進位資料損壞。
    try:
        REDIS_POOL = redis.ConnectionPool(host=redis_host,
                                          port=int(redis_port),
                                          password=redis_password,
                                          decode_responses=False)
        r = redis.Redis(connection_pool=REDIS_POOL)
        r.ping()  # 測試連線
    except Exception as e:
        print(f"Error when connecting to Redis. Error msg: {e}!")
        if "/opt/airflow" in sys.path:
            raise AirflowException
        raise Exception
    else:
        if r is None:
            print("Redis client is None, skipping cache")
            return None
        print("Successfully connect to Redis!")
        return r
