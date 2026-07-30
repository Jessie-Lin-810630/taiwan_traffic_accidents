# ADR-0001：不使用 AirflowException，例外一律原樣拋出

- 日期：2026-07-29
- 狀態：已採納
- 範圍：`src/util/` 新版連線工具、`src/task/` 全部 20 個 ETL 模組

## 背景

`src/util/logger_crtx.py` 原本回傳一個三欄位的 `LoggerContext`
（`logger`、`is_airflow_env`、`AirflowException`），讓每個模組自行決定：

```python
except SQLAlchemyError as e:
    logger.error(...)
    if is_airflow_env:
        raise AirflowException("Airflow Task Failed: ...") from e
    raise
```

`mysql_utils.py`、`redis_utils.py`、`crawling_utils.py` 三個模組合計有 32 處這種分支。
`src/task/` 底下另有 32 處直接 `raise AirflowException`（且丟的是類別而非實例，
原始錯誤訊息全失）。

## 決策

**不再依執行環境切換例外型別。所有失敗一律 `logger.error(..., exc_info=True)` 後 `raise`。**

`logger_crtx` 隨之收斂為單一函式 `get_logger(name)`，不再偵測 Airflow、
不再回傳例外類別。`src/util/` 底下再也沒有任何模組知道 Airflow 存在。

## 理由

1. **在 Airflow 3 中，`AirflowException` 沒有任何特殊語意。** 它只是 `Exception` 的子類別。
   任何未被捕捉的例外都會使 task 失敗，並依 `default_args` 的 `retries` 重試 ——
   拋 `AirflowException` 與拋原始例外，對 task 的結果完全相同。
   真正會改變行為的是 `AirflowFailException`（不重試）與 `AirflowSkipException`（跳過），
   而本專案一次都沒有使用。

2. **轉換例外型別會破壞除錯資訊。** 即使正確使用 `raise ... from e`，
   Airflow UI 上第一眼看到的仍是被包裝過的訊息，原始的 `SQLAlchemyError`
   或 `requests.exceptions.Timeout` 退居 `__cause__`。原樣拋出讓例外型別本身就是診斷資訊。

3. **這個分支讓模組無法在 Airflow 之外被匯入。** Cloud Run 的 Streamlit 容器
   （`python:3.12` base image）與 pytest 都沒有安裝 airflow。
   環境偵測本身也是脆弱的：`logger_crtx` 原本用
   `"opt/airflow" in sys.path` 判斷，但 `sys.path` 內是絕對路徑 `/opt/airflow`，
   該條件永遠為 `False`。

4. **`is_airflow_env` 通不過 deletion test。** 移除它之後，複雜度沒有跑到別處：
   `logging.basicConfig()` 的守衛 `if not logging.getLogger().handlers`
   在 Airflow 容器內本來就會因 root logger 已有 handler 而跳過，不需要環境偵測。

## 後果

- `src/util/` 的三個連線模組終於可在地端、pytest、Cloud Run 被匯入
  （先前因 `logger, is_airflow_env = create_logging_logger()` 以 2 個變數
  解包 3 欄位 NamedTuple，在任何環境匯入都會 `ValueError`）。
- Airflow task 的失敗與重試行為不變。
- 日誌訊息不再重複夾帶 `{e}` —— `exc_info=True` 已提供完整 traceback。
- 若日後真的需要「失敗但不重試」，正確作法是在該處明確拋出 `AirflowFailException`，
  而不是恢復本 ADR 廢除的環境分支。

## 套用紀錄

- `src/util/`：`mysql_utils`、`redis_utils`、`crawling_utils` 共 32 處環境分支移除。
- `src/task/`：20 個 ETL 模組移除 `Variable`（20 次匯入、0 次使用）與 `AirflowException`；
  29 處裸 `raise AirflowException` 改為 `raise`；`e_crawling_nightmarket.py` 唯一帶訊息的
  原始拋出（缺少 Google Maps API 金鑰）改為 `ValueError`；約 109 處 `print()` 改為
  `logger.info()` / `logger.error(..., exc_info=True)`。

## 尚未套用之處

- `dags/d04_analysis_pedestrian_accidents.py` 仍有 `raise AirflowException(...)`。
  DAG 檔本來就在 Airflow 內執行，優先度低，但同樣沒有理由轉換例外型別。
- `src/util/create_db_engine_or_database.py`（含 `get_or_set_cache_from_redis.py`、
  `inspect_table_schema.py`）刻意凍結，待新版連線工具接上後整批刪除。
