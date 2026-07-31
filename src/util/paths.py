"""專案內所有路徑的單一基準（ADR-0007）。

以 `__file__` 推導專案根，**不使用 `Path().resolve()`（CWD）** ——
CWD 是行程的工作目錄而非專案根，只有「從專案根執行」時兩者才碰巧相同。

`parents[2]` 在三個環境都成立，因為 `src/` 一律直接位於根之下：

| 環境 | 本檔位置 | PROJECT_ROOT |
| --- | --- | --- |
| 地端 | `<repo>/src/util/paths.py` | `<repo>` |
| Airflow 容器 | `/opt/airflow/src/util/paths.py`（`./src` 掛載） | `/opt/airflow` |
| Cloud Run | `/app/src/util/paths.py`（`COPY ./src ./src`） | `/app` |

本模組**只計算路徑、不建立目錄** —— Streamlit 容器載入它時不該憑空建出 `data/`。
建目錄是實際要寫檔的函式的職責。
"""

from pathlib import Path

# 本檔位於 <root>/src/util/paths.py，故往上三層為專案根。
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 資料落點：執行環境的產物，與程式碼分離。
# 容器內由 docker-compose 掛載 ./data:/opt/airflow/data，地端與容器共用同一份。
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# 程式碼資產：與程式碼同進退（同一個 commit、同一個映像），
# 因此以本檔位置推導而非專案根 —— 即使 src/ 被搬到別處也仍然正確。
MART_SQL_DIR = Path(__file__).resolve().parents[1] / "task" / "mart_table_sql"
