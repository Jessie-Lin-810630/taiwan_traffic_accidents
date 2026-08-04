import os
from urllib.parse import quote_plus

import pymysql
from pymysql.connections import Connection
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# pymysql用
username = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
host = os.getenv("MYSQL_HOST", "mysql8")
port = os.getenv("MYSQL_PORT", 3306)
default_database = os.getenv("MYSQL_DATABASE")
charset = os.getenv("CHARSET", "utf8mb4")
writer = quote_plus(os.getenv("AIRFLOW_ADMIN_EMAIL", "jessie"))


def get_conn_pymysql(database: str = default_database) -> Connection:
    conn = pymysql.connect(
        host=host,
        port=int(port),
        user=username,
        password=password,
        database=database,
        charset=charset,
        autocommit=False,
        connect_timeout=60,  # 連線建立超時
        read_timeout=600,  # 讀取超時（適合大查詢）
        write_timeout=600,  # 寫入超時
    )
    return conn


def get_engine_sqlalchemy(database: str = default_database) -> Engine:
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset={charset}",
        echo=False,  # 使用預設值，不印SQL日誌，保持乾淨輸出，生產環境適用
        pool_size=5,
        pool_pre_ping=True,  # 檢查連線有效性
        pool_recycle=300,  # 每5分鐘自動重整連線，可再調整
        connect_args={"connect_timeout": 60},
    )
    return engine
