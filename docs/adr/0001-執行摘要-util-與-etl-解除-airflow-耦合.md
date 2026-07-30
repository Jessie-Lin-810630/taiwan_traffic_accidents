# 執行摘要：util 與 ETL 解除 Airflow 耦合

- 日期：2026-07-30
- 分支：`refactor/util-connection-layer`
- 依據：[ADR-0001 不使用 AirflowException，一律原樣拋出](./0001-不使用-airflowexception-一律原樣拋出.md)
- 性質：**interim execution report** —— 記錄 ADR-0001 實際套用到哪裡、還有哪些同病因的地方尚未處理

本次工作來自一輪架構檢視，該檢視提出 8 個候選項目，本文件涵蓋其中兩項：

| 候選 | 標題 | 狀態 |
| --- | --- | --- |
| 候選 2 | 收斂 `LoggerContext`，雙環境 seam 只留一個形狀 | 已完成 |
| 候選 3 | 拆掉 `src/task/` 20 個模組對 Airflow 的 import 綁定 | 已完成 |

範圍另追加 `dags/`：`d01`／`d06` 的死 import 與 `d04` 的例外轉換，
病因與候選 2／3 相同，只是不在最初劃定的目錄內（見第四章第 3 點）。

套用完成後，全 codebase 的 `AirflowException` 只剩
`src/util/create_db_engine_or_database.py`（依決策 5 凍結）。

---

## 一、決策摘要

改動前，`logger_crtx.create_logging_logger()` 回傳三欄位 NamedTuple
（`logger`、`is_airflow_env`、`AirflowException`），每個模組自行以
`if is_airflow_env: raise AirflowException(...) from e` 分流。

逐項討論後的定案：

| # | 決策 |
| --- | --- |
| 1 | 拿掉依環境選例外的整條邏輯，一律 `raise`。Airflow 3 中 `AirflowException` 與其他例外的失敗／重試語意完全相同 |
| 2 | 刪除環境偵測本身。`basicConfig()` 的守衛 `if not logging.getLogger().handlers` 已自我修正，不需要偵測環境 |
| 3 | 刪除 `set_loguru_logger()` 與 `from loguru import logger` —— loguru 不在 `pyproject.toml` 也不在 `requirements.txt`，且零真實呼叫端 |
| 4 | 範圍限定 `src/util/` 四個檔案，不擴及 `src/task/`（候選 3 另行處理） |
| 5 | 舊版連線工具三檔凍結，待新版接上後整批刪除 |
| 6 | 檔名保留 `logger_crtx.py`，函式 `create_logging_logger` → `get_logger(name)` |
| 7 | 以 pytest 冒煙測試把關，不只手動 import 檢查 |

候選 3 追加兩項：

| # | 決策 |
| --- | --- |
| 8 | `print()` 一併改為 `logger`，完整落實 ADR-0001 的 `logger.error(..., exc_info=True)` pattern |
| 9 | `e_crawling_nightmarket.py` 唯一帶訊息的原始拋出（缺 Google Maps API 金鑰）改為 `ValueError` |

---

## 二、實際改動

### 候選 2 — `src/util/`（commit `1293c77`）

```
src/util/logger_crtx.py       101 行 → 33 行
src/util/mysql_utils.py       修正解包 + 移除  7 處 if is_airflow_env
src/util/redis_utils.py       修正解包 + 移除  8 處 if is_airflow_env
src/util/crawling_utils.py    改寫 _ctx 解包 + 移除 17 處 if is_airflow_env
```

`logger_crtx.py` 現在的全貌是一個純標準函式庫函式：

```python
def get_logger(name: str) -> logging.Logger:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="...")
    return logging.getLogger(name)
```

**順帶修好的既存缺陷**

- `mysql_utils.py:16` 與 `redis_utils.py:13` 以 2 個變數解包 3 欄位 NamedTuple，
  **在任何環境 import 都會 `ValueError`**。這是新版連線工具遲遲接不上的直接原因。
- `logger_crtx` 原本的 `"opt/airflow" in sys.path` 判斷永遠為 `False`
  （`sys.path` 內是絕對路徑 `/opt/airflow`）。
- 地端分支回傳 `logging.getLogger(__name__)`，其中 `__name__` 是 `src.util.logger_crtx`，
  導致**所有模組的日誌都被標成 logger_crtx**。
- `loguru` 為 module-top import 卻不在任一份依賴清單，兩個容器重建都會
  在 import `logger_crtx` 的當下 `ModuleNotFoundError`。

### 候選 3 — `src/task/`（commit `3d41e0b`）

20 個 ETL 模組（`e_*` 2、`t_*` 8、`l_*` 8、`create_*` 2）：

| 項目 | 數量 |
| --- | --- |
| 移除 `from airflow.models import Variable` | 20 檔（**0 次使用**，純 import 期耦合） |
| 移除 `from airflow.exceptions import AirflowException` | 20 檔（其中 8 個 `t_*.py` 根本沒有 try/except） |
| 裸 `raise AirflowException` → `raise` | 29 處 |
| 帶訊息的原始拋出 → `ValueError` | 1 處（`e_crawling_nightmarket.py`，缺 API 金鑰） |
| `print()` → `logger.info()` / `logger.error(..., exc_info=True)` | 約 109 處 |
| 補上模組 docstring（D100） | 20 檔 |
| 補上函式 docstring（D419 空 docstring／D103） | 11 處 |
| 修正 D205 摘要行空行 | 15 處 |
| 裸 `except:` → `except Exception:`（E722） | 1 處 |

### 收尾修補（尚未 commit）

| 檔案 | 改動 |
| --- | --- |
| `src/util/mysql_utils.py:141` | 遺漏的 `print(schema[...])` → `logger.info(...)` |
| `dags/d01`、`dags/d06` | 移除從未使用的 `Variable` 與 `AirflowException` import（與候選 3 的 `Variable` 同型） |
| `src/task/create_night_markets_tables.py`、`create_traffic_accident_tables.py`、`src/util/mysql_utils.py` `create_tables()` | `CREATE TABLE` 由 `engine.connect()` 改為 `engine.begin()` |
| `dags/d01`、`d05`、`d06` | 補模組／函式 docstring、清除未使用的區域變數，以通過 pre-commit |
| `dags/d04` | 套用 ADR-0001（見下） |

### `dags/d04` —— ADR-0001 的最後一處套用

此檔原先被歸類為「其他候選的範疇」，屬**分類錯誤**：`d04` 的 3 處
`raise AirflowException(...)` 正是 ADR-0001 所針對的病因本身，
與候選 2／3 完全同源，只是不在最初劃定的 `src/util/`／`src/task/` 範圍內。
若留待其他候選處理，極可能被遺漏。

| 位置 | 改動 |
| --- | --- |
| `:37` | `raise AirflowException(".sql files not found...")` → `raise FileNotFoundError(...)`（原始拋出，非 re-raise，比照 `e_crawling_nightmarket` 改用 `ValueError` 的前例） |
| `:49` | `except` 內的 `raise AirflowException(f"SQL執行失敗: {line_error}")` → `logger.error(..., exc_info=True)` + `raise` |
| `:92` | `except` 內的 `raise AirflowException(f"處理第{i+1}份...")` → `logger.error(..., exc_info=True)` + `raise` |
| import 區 | 移除 `AirflowException`、從未使用的 `Variable`／`TaskGroup`／`text`；加入 `get_logger` |
| 5 處 `print` | → `logger.info()` |

**一處超出純 ADR-0001 範圍的小幅擴張**：`:92` 原訊息引用迴圈變數 `i`，
`sql_file_paths` 為空時會 `UnboundLocalError` 遮蔽真因。改寫該行必須決定訊息內容，
因此於 `try` 之前加入 `file_path = None`，讓訊息改為指出失敗的檔案路徑，
順帶消除該潛在崩潰。此為 1 行改動。

---

## 三、驗證方式

```bash
poetry run pytest test/unit_test/          # 6 passed
poetry run pre-commit run --files <改動檔案>  # ruff check / ruff format 皆 Passed
```

`test/unit_test/test_util_logger_crtx.py`（commit `ead3d8e`）釘住六件事：

1. `get_logger` 以呼叫端傳入的名稱命名記錄器
2. root logger 已有 handler 時不重複 `basicConfig`（等同 Airflow 容器內情境）
3. `src/util/` 三個連線模組可在無 airflow 的環境匯入
4. 這三個模組不再持有 `AirflowException` / `is_airflow_env`
5. **`src/task/` 全部 20 個 ETL 模組可在無 airflow 的環境匯入**
6. 這 20 個模組不再持有 `AirflowException` / `Variable`

此外以 AST 逐一比對過：

- 20 個模組的所有 top-level 函式名稱與參數與改動前完全一致
- 所有多行字串字面值（SQL DDL/DML）內容一致，差異僅為行尾空白被 pre-commit 移除
- `dags/` 從 `src.` 匯入的每一個名稱在來源模組皆存在

---

## 四、過程中發生的意外（已修正，記錄以供追溯）

### 1. 誤改 12 個範圍外檔案

為取得 lint 清單時，`pre-commit run --files` 帶了過寬的檔案清單，導致
`ruff format` 與 `ruff check --fix` 修改了 12 個不在範圍內的檔案，
其中包含**決策 5 明訂凍結的** `create_db_engine_or_database.py`。
已全數 `git restore` 還原。

### 2. `ruff check --fix` 移除既存死 import 造成 DAG 匯入回歸

`ruff --fix` 在 20 個 ETL 模組中移除了 37 個既存的未使用 import。
其中 `create_traffic_accident_tables.py` 與 `create_night_markets_tables.py`
的 `create_engine_to_mysql` / `create_database` 雖在該模組內未使用，
卻是 **`dags/d01` 與 `dags/d05` 賴以轉出口的名稱** ——
移除後兩個 DAG 會在解析期 `ImportError`。

修正方式：兩個 DAG 改為直接從 `src.util.create_db_engine_or_database` 匯入。
這本來就是正確的相依方向，DAG 不應透過 task 模組取得 util 函式。

**教訓**：只比對函式簽章不足以確保相容性，**模組層級的轉出口名稱也是介面的一部分**。

### 3. 將 `dags/d04` 誤分類為「其他候選的範疇」

初版報告把 `d04` 的 3 處 `raise AirflowException(...)` 列在第五章 C 類，
理由是「DAG 本來就跑在 Airflow 裡，優先度低」。這是錯的 ——
**病因判定不應受檔案所在目錄影響**。經指出後已納入本輪一併修復。

**教訓**：劃定範圍時用的是目錄（`src/util/`、`src/task/`），
但 ADR 的適用對象是**病因**。兩者不一致時，應以病因為準，
否則同一個問題會散落在多個候選之間而被遺漏。

### 4. D205 修正腳本誤判

補 docstring 空行的腳本誤將結尾的 `""")  # 註解` 判定為 docstring 開頭，
插入 13 行雜訊；修正時又多刪了合法空行。最終以 `ruff format` 收斂，
並以 AST 比對確認 SQL 字串與函式簽章未受影響。

---

## 五、同病因但尚未修復的位置

以下全部**未修改**，依病因分類。三類的處置理由不同。

### A. 依決策 5 刻意凍結 —— 待新版連線工具接上後整批刪除

| 檔案 | 病徵 |
| --- | --- |
| `src/util/create_db_engine_or_database.py` | 3 處 `AirflowException`（`:14` 條件式 import、`:104`、`:124`）、4 處裸 `raise Exception`（`:105`、`:125` 丟類別不丟實例，原始錯誤全滅）、3 處 `"/opt/airflow" in sys.path` 環境偵測（`:12`、`:103`、`:123`）、5 個 `print`、0 logger |
| `src/util/get_or_set_cache_from_redis.py` | 5 個 `print`、0 logger；`set_cache`／`get_cache`／`delete_cache` 三個函式**完全吞掉 Redis 例外**，呼叫端無從得知快取層故障 |
| `src/util/inspect_table_schema.py` | 7 個 `print`、0 logger；`:8` 於 **module import 期就建立 engine**，匯入該模組即嘗試連線 MySQL |

**這三個檔案才是現行生產環境實際使用的那一套**（8 個 loader、`dags/d04` 都 import 它）。
凍結是為了避免兩套同時處於半遷移狀態，但在新版接上之前，
上述缺陷一直存在於生產路徑。

已知的接上阻礙：

- `mysql_utils.upsert_to_table()` 的表名與 `update_part` 仍硬編碼為 `dim_accident_day`，
  只能取代 `l_dim_accident_day`，無法服務其餘 7 個 loader
- `mysql_utils.create_tables()` 只含 2 段 DDL 且欄位名與現行不符，是模板而非替代品
- `crawling_utils.download_and_extract_zip()` 的參數順序多了 `headers`，
  與現有呼叫端不相容

### B. 孤兒模組 —— 建議先確認是否該刪，而非投資修復

| 檔案 | 病徵 | 呼叫端 |
| --- | --- | --- |
| `src/util/convert_time_zone.py` | 2 個 `print`、0 logger | **0** |
| `src/util/validate_csv_encoding.py` | 3 個 `print`、0 logger | **0** |
| `src/util/inspect_table_schema.py` | 見 A 類 | **0** |

三者皆無任何 import。`inspect_table_schema.py` 的功能已被
`mysql_utils.inspect_table()` 吸收。

### C. 屬於其他候選項目的範疇

| 位置 | 病徵 | 歸屬 |
| --- | --- | --- |
| `src/task/core/c_data_service.py` | 19 個 `print`、0 logger；例外全部吞成空 `DataFrame` 或空 list（`:52`、`:157`、`:213`、`:262`），另有 4 處「快取寫入失敗僅記錄不中斷」。**MySQL 全掛時 `d06` 六個 task 仍全綠，Redis 被寫入一堆空結果** | 候選 5 |
| `src/task/core/c_db.py` | 零例外處理；`:38-40` 以 f-string 內插日期組 SQL，`params` 參數接收後未使用 | 候選 5 |
| `dags/d04` `read_sql()` | pymysql `autocommit=False` 卻**沒有任何 `conn.commit()`**。目前僅靠 MySQL 對 DDL 的隱式提交才生效（mart SQL 幾乎全是 DDL，唯一的 `UPDATE` 位於 `CREATE PROCEDURE` 內、其後有 `DROP TABLE` 觸發隱式提交）。**日後若 mart SQL 結尾出現純 DML，會靜默 rollback 且 task 仍為綠燈** | 候選 4 |
| `dags/d04` `exec_sql_linebyline()` | `finally` 內有 `return None`，**會吞掉本次剛改成 `raise` 的例外**，使該處的例外處理形同虛設；`except` 區塊引用可能未綁定的 `conn`。（此函式為死碼，呼叫點已註解） | 候選 4 |
| `dags/d04` `read_sql()` | `conn`／`cursor` 建立於 `try` 之外，連線失敗未被接住 | 候選 4 |
| `src/task/e_crawling_nightmarket.py:125-126` | `find_tw_night_markets_list()` 的 `finally: return str(csvfile_name)` **會吞掉同函式四個 `except` 分支剛拋出的例外**，函式永遠「成功」並回傳可能不存在的檔案路徑 | 候選 4 |
| `src/task/l_*.py` 8 檔 | `finally: if conn: cursor.close()` —— 以 `conn` 守衛 `cursor`。若 `conn.cursor()` 自身失敗，`cursor` 仍為 `None`，會拋 `AttributeError` 遮蔽真正的錯誤 | 候選 1 |
| `src/task/e_*.py` | 非 200 狀態碼僅 `return None`，呼叫端接著 `.extend(None)` 觸發 `TypeError`；Google Places API 的 `OVER_QUERY_LIMIT`（HTTP 200）被當成「找不到地點」；約 300 次連續 API 呼叫無重試、無節流 | 候選 6 |
| `src/task/e_*.py`、`t_fact_night_markets.py`、`dags/d04:98` | 以 `Path().resolve()` 組路徑，落點隨行程 CWD 漂移（地端為 repo 根目錄、容器內為 `/opt/airflow`）；`d04` 更在 DAG 解析期就計算路徑 | 候選 8 |
| 全 codebase | 設定散在 3 個 `load_dotenv()` 與多組 module-level 全域變數，零驗證。`MYSQL_USER` 未設時會被內插成字面字串 `"None"` 進 DSN，錯誤表現為認證失敗而非設定缺漏。唯一有驗證的變數是 `GOOGLE_MAP_API_KEY` | 候選 7 |

### D. 既存 lint 債（本次未處理）

`dags/d02`、`d03`、`d04` 與 `src/task/core/`、`src/pages/`、`src/app.py`
從未通過 pre-commit（`ruff` 是上上個 commit 才引入）。
主要為 D100／D103 缺 docstring、`I001` import 未排序、
`F401` 未使用匯入、`F841` 未使用區域變數。
依「只碰必要之處」原則，本次僅修正實際被改動的檔案。

---

## 六、建議的下一步

1. **`dags/d04` 補上 `conn.commit()`** —— 一行，且是目前唯一靠隱式提交撐住的寫入路徑
2. **候選 1**（8 個 loader 收斂為一個 upsert module）—— 同時解掉 A 類的接上阻礙與 `cursor` 守衛缺陷
3. **候選 5**（快取層不再吞例外）—— 讓 `d06` 的監控恢復意義
4. 確認 B 類三個孤兒模組是否可刪
