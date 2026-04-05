# 資料表檢查工具
# 開發除錯用 - 自動印出資料表的 Schema (欄位與型態)、資料總筆數，以及前 3 筆預覽資料
from src.util.create_db_engine_or_database import create_engine_to_mysql
from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection
import pandas as pd

engine = create_engine_to_mysql("traffic_accidents")


def inspect_table(engine: Engine, db_name: str, table_name: str) -> None:
    full_table_path = f"`{db_name}`.`{table_name}`"
    print(f"\n{'='*30} Checking Table: {full_table_path} {'='*30}")

    def _extracted_from_inspect_table_7(full_table_path: str,
                                        conn: Connection) -> str | pd.DataFrame:
        # 1. 型態與索引檢查
        print("[1. Schema Definition]")
        schema = pd.read_sql(text(f"DESC {full_table_path}"), conn)
        print(schema[['Field', 'Type', 'Key']])

        # 2. 筆數統計
        count = conn.execute(text(f"SELECT COUNT(*) FROM {full_table_path}")).scalar()
        print(f"[2. Total Rows: {count:,}]")

        # 3. 資料預覽 (limit 5)
        data = pd.read_sql(text(f"SELECT * FROM {full_table_path} LIMIT 3"), conn)
        print("[3. Data Preview]:")

        # 4. 檢視回傳結果
        if data.empty:
            return " This table is currently empty."
        else:
            return data

    try:
        with engine.connect() as conn:
            result = _extracted_from_inspect_table_7(full_table_path, conn)
    except Exception as e:
        print(f"Error inspecting {full_table_path}: {e}")
    else:
        print(result)
        return None
