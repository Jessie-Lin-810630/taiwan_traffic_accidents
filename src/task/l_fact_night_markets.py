"""Load 階段：將夜市事實資料 upsert 進 `fact_night_markets`。"""

from datetime import datetime

import pandas as pd

from src.util.create_db_engine_or_database import get_pymysql_conn_to_mysql
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)


def l_fact_night_markets(
    df_fact_night_markets: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將夜市事實資料寫入 `fact_night_markets`。

    採用 INSERT ... ON DUPLICATE KEY UPDATE，發生錯誤時復原事務並原樣拋出例外。

    Parameters:
        df_fact_night_markets (pandas.DataFrame): 待寫入的夜市事實資料。
        database (str | None): 目標資料庫名稱。
    """
    df_fact_night_markets["updated_on"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 準備INSERT資料表時需要的SQL語句，採用UPSERT
    # 先準備INSERT部分
    columns = ", ".join(df_fact_night_markets.columns)
    placeholders = ", ".join(["%s"] * len(df_fact_night_markets.columns))

    # 準備UPDATE的部分：
    update_part = """updated_on=VALUES(updated_on),
    business_hours_closing=VALUES(business_hours_closing),
    business_hours_opening=VALUES(business_hours_opening)"""

    # 組合出完整SQL語句
    dml_str = f"""INSERT INTO fact_night_markets ({columns})
                  VALUES ({placeholders})
                  ON DUPLICATE KEY UPDATE {update_part};
                """
    # 6. 寫入資料表
    logger.info("====Inserting into table `fact_night_markets`....====")
    conn = None
    cursor = None
    try:
        conn = get_pymysql_conn_to_mysql(database)
        if conn:
            cursor = conn.cursor()
            cursor.executemany(dml_str, df_fact_night_markets.values.tolist())
            conn.commit()
    except Exception:
        logger.error("Error on inserting into table.", exc_info=True)
        if conn:
            conn.rollback()
        raise
    else:
        logger.info("====Successfully inserting into table `fact_night_markets`====")
    finally:
        if conn:
            cursor.close()
            conn.close()
    return None
