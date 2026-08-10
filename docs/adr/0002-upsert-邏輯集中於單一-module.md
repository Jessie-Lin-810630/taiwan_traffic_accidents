# ADR-0002：upsert 邏輯集中於單一 module，loader 只保留宣告

- 日期：2026-07-30
- 狀態：已採納
- 範圍：`src/util/mysql_utils.py`、`src/task/l_*.py` 8 個檔案
- 相關：[ADR-0001 不使用 AirflowException，一律原樣拋出](./0001-不使用-airflowexception-一律原樣拋出.md)

## 背景

`src/task/` 下 8 個 loader（`l_dim_accident_day`、`l_dim_accident_type`、
`l_dim_lane_design`、`l_dim_road_design`、`l_fact_accident_env`、
`l_fact_accident_human`、`l_fact_accident_main`、`l_fact_night_markets`）
各自持有一段逐字元相同的 upsert 實作：組 SQL、開連線、`executemany`、
commit、except 中 rollback、finally 中關閉游標與連線。

以 `diff` 比對，8 份之間的差異只有四項：

1. 函式名與 DataFrame 參數名
2. 目標表名
3. `ON DUPLICATE KEY UPDATE` 要更新的欄位（7 份是單一欄位，`l_fact_night_markets` 是三欄）
4. `l_fact_night_markets` 在寫入前額外設定 `updated_on` 時間戳

其餘約 20 行的錯誤處理與資源管理完全重複。

同一段程式碼複製 8 份，代價不只是行數：

- **修一個錯要修 8 次。** 例如 `finally: if conn: cursor.close()` 以 `conn` 守衛 `cursor`，
  若 `conn.cursor()` 自身失敗，`cursor` 仍為 `None`，會拋 `AttributeError`
  遮蔽真正的錯誤 —— 這個缺陷 8 份全中。
- **測試要寫 8 份**，或者（如現況）一份也沒有。

## 決策

**upsert 的實作集中到 `mysql_utils.upsert_to_table()`；8 個 loader 只保留「這張表怎麼 upsert」的宣告。**

```python
def upsert_to_table(
    df: pd.DataFrame,
    table: str,
    update_columns: list[str],
    database: str | None = None,
) -> None:
```

loader 瘦成：

```python
def l_dim_accident_day(df_dim_accident_day, database=None) -> None:
    """以 UPSERT 將事故日維度資料寫入 `dim_accident_day`。"""
    upsert_to_table(
        df_dim_accident_day,
        table="dim_accident_day",
        update_columns=["accident_weekday"],
        database=database,
    )
```

### 四項子決策

| # | 決策 | 理由 |
| --- | --- | --- |
| 1 | **保留 8 個 loader 檔案**，不讓 DAG 直接呼叫 `upsert_to_table` | `l_` 層是 `CLAUDE.md` 記載的 e_／t_／l_ 分層慣例的一部分，也是「這張表怎麼寫入」的單一宣告點。刪掉它們會把表名與欄位清單散到 6 個 DAG 裡，且 DAG 需改 import 與呼叫 |
| 2 | `ON DUPLICATE KEY UPDATE` 的介面收 **欄位名 `list[str]`**，而非完整 SQL 片段 | 呼叫端寫不出 SQL 語法錯誤；且欄位 list 足以表達全部 8 個案例（含三欄的夜市表）。傳字串的話介面沒有真的變淺 |
| 3 | **接上新版連線工具** —— `upsert_to_table` 使用 `mysql_utils.get_pymysql_conn_to_mysql` | 這是 ADR-0001 決策 5 所說「待新版接上後刪除舊版」的第一步。新版具備 typed exception、`exc_info=True`、守衛正確的 `finally` |
| 4 | `l_fact_night_markets` 的 `updated_on` 時間戳**留在該 loader 內**，不進 `upsert_to_table` | 只有一個呼叫點需要。為單一呼叫點在共用介面上加參數，是假設性的 seam 而非真實的 seam |
| 5 | **`create_tables()` 改為通用 DDL 執行器**，兩支 `create_*_tables.py` 保留 DDL 宣告並呼叫它 | 見下方「決策 5：範圍擴張至 DDL」 |

### 決策 5：範圍擴張至 DDL

原本規劃「`mysql_utils.create_tables()` 保留不碰」，實作前盤點時發現
`create_night_markets_tables.py` 與 `create_traffic_accident_tables.py`
的 `except/finally` 區塊**逐字元相同**，且 `create_tables()` 是同一件事的第三份實作 ——
與「`upsert_to_table` 通用版 vs 8 份 loader」是完全相同的結構。

改動的直接動機是**現行兩支的日誌有業務語意不精確的問題**：
`CREATE TABLE IF NOT EXISTS` 在資料表已存在時不報錯，
但程式碼緊接著無條件印出 `Table 'x' created successfully.`，對除錯的人是誤導。
`create_tables()` 具備 `inspect_table_exists()` 前置檢查與 `SQLAlchemyError` 分流，
能區分「已存在、略過」與「本次建立」。

因此 `create_tables()` 移除硬編碼 DDL（其欄位名 `date` / `weekday`
與現行綱要 `accident_date` / `accident_weekday` 本就不符），改收 `tables: dict[str, str]`。

## 後果

- 錯誤處理與資源管理集中一處：修一次，8 個呼叫點同時受惠。
- 順帶修掉 `finally` 以 `conn` 守衛 `cursor` 的缺陷（新版 `upsert_to_table` 對
  `cursor` 與 `conn` 各自守衛，且關閉失敗只降級為 `logger.warning`，不覆蓋主例外）。
- 測試只需針對一個介面撰寫。
- 8 個 loader 檔案從約 60 行縮到約 15 行。
- 舊版 `create_db_engine_or_database.get_pymysql_conn_to_mysql` 在此之後
  **不再有任何呼叫端**（舊版模組其餘函式仍被 `dags/d01`、`d04`、`d05` 與
  `get_table_from_sql_server` 使用，尚不能整檔刪除）。

### 誠實的限制

瘦身後的 8 個 loader 仍然是 shallow module —— 介面（函式名 + 兩個參數）
與實作（一次函式呼叫）幾乎一樣寬。它們通不過嚴格的 deletion test：
刪掉之後複雜度不會重新出現在 8 個地方，只會搬進 DAG。

保留它們是**基於分層慣例與 DAG 穩定性的取捨**，不是因為它們夠深。
若日後 `dags/` 有大規模重整，應重新檢視這一層是否還值得存在。

## 未納入本次範圍

- `crawling_utils.download_and_extract_zip()` 的參數順序與現有呼叫端不相容，
  屬於新版連線工具接上的另一個阻礙，與本 ADR 無關。
- `src/task/t_*.py` 8 支的 CSV 讀取骨架同樣重複，但它們零 `try/except`、
  零資源管理，缺少本 ADR 病因的另一半；且各檔清洗邏輯互異。建議另開候選評估。

實際執行結果與完整盤點見
[ADR-0002 執行摘要](./0002-執行摘要-資料庫寫入樣板集中化.md)。
