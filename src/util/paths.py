"""定義專案內所有路徑常數，是全專案取得路徑的唯一來源。

路徑一律由本檔的 `__file__` 往上推導，因此不論從哪個工作目錄啟動程式，
取到的路徑都相同。`src/` 一律直接位於專案根之下，故 `parents[2]` 在三種
執行環境都指向正確的根目錄：

| 環境 | 本檔位置 | PROJECT_ROOT |
| --- | --- | --- |
| 地端 | `<repo>/src/util/paths.py` | `<repo>` |
| Airflow 容器 | `/opt/airflow/src/util/paths.py`（`./src` 掛載） | `/opt/airflow` |
| Cloud Run | `/app/src/util/paths.py`（`COPY ./src ./src`） | `/app` |

本模組只計算路徑、不建立目錄。

Attributes:
    PROJECT_ROOT (Path): 專案根目錄。
    DATA_DIR (Path): 資料目錄 `<root>/data`。
    RAW_DATA_DIR (Path): 抓取到的原始檔落點 `<root>/data/raw`。
    PROCESSED_DATA_DIR (Path): 清洗後檔案落點 `<root>/data/processed`。
    MART_SQL_DIR (Path): mart 層 SQL 腳本目錄 `<src>/task/mart_table_sql`。

Notes:
    路徑取得方式參考 ADR-0007。
"""

from pathlib import Path

# 本檔位於 <root>/src/util/paths.py，故往上三層為專案根。
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 資料落點：執行環境的產物。
# 容器內由 docker-compose 掛載 ./data:/opt/airflow/data。
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# 程式碼資產：
MART_SQL_DIR = Path(__file__).resolve().parents[1] / "task" / "mart_table_sql"
