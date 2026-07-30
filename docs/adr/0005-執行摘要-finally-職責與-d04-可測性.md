# 執行摘要：`finally` 職責與 `d04` 的可測性

- 日期：2026-07-30
- 分支：`refactor/util-connection-layer`
- 依據：[ADR-0005 `finally` 只負責釋放資源](./0005-finally-只負責釋放資源.md)
- 前一輪：[ADR-0004 執行摘要](./0004-執行摘要-mysql-engine-生命週期.md)
- 性質：**interim execution report**

| 候選 | 標題 | 狀態 |
| --- | --- | --- |
| 候選 4 | `finally: return` 吞例外 | 已完成 |

候選 4 由 ADR-0003 執行摘要第五章 A 提出，ADR-0004 執行摘要第七章列為下一步第 1 項。

**範圍擴張**：本輪含一項超出原病因的工作 ——
**`d04` 的三支函式搬遷至 `src/task/exec_mart_sql.py`**。
這不是順手重構，而是本輪修正能被測試釘住的**前提**：
三支函式原本定義在 `@dag` 函式體內，外部無法 import。

---

## 一、病因定義

候選 4 的病因是：

> **`finally` 區塊承擔了「釋放資源」以外的職責，
> 使正在傳播的例外被丟棄或取代。**

三個可檢驗的判準：

1. **`finally` 內有 `return`** —— 丟棄正在傳播的例外
2. **清理動作無守衛** —— `close()` 自身拋例外會取代原例外
3. **資源變數的綁定時機與 `finally` 的假設不符** —— `UnboundLocalError` 遮蔽真正的錯誤

實際掃描發現的情形：

| 位置 | 判準 1 | 判準 2 | 判準 3 |
| --- | --- | --- | --- |
| `find_tw_night_markets_list` | ✓ `finally: return str(csvfile_name)` | — | — |
| `exec_sql_linebyline` | ✓ `finally: return None` | ✓ 裸 `close()` | ✓ `conn` 綁定於 `try` 內 |
| `read_sql` | — | ✓ 裸 `close()` | — |

淨結果：**維基百科不可達時，`find_tw_night_markets_list` 的四個 `except` 分支
各自寫了 `raise`，卻沒有任何一個能傳出去** —— 呼叫端拿到的是一個字串路徑，
而該路徑指向一個從未被寫出的檔案。`d05` 的下一個 task 會拿著它去開檔。

同一支函式另有兩條「沒寫檔卻回傳路徑」的路徑：非 200 回應（`soup` 保持 `None`，
跳過解析與寫檔）、以及 `df.empty`（只記一行 log 後照樣寫出只有標題列的 CSV）。

---

## 二、決策摘要

| # | 決策 | 狀態 |
| --- | --- | --- |
| 1 | 三處全數納入本輪 | 依原案 |
| 2 | `exec_sql_linebyline` 修好但不接回呼叫鏈 | 依原案 |
| 3 | `find_tw_night_markets_list` 的 `return` 移至 `else` 尾端；非 200 與空 `df` 皆 `raise` | 依原案 |
| 4 | 守衛抽成 `mysql_utils.close_quietly()` 由兩個模組共用；`conn` / `cursor` 初始化為 `None` 並在 `try` 內綁定 | **調整**（原案為各自手寫） |
| 5 | `d04` 三支函式搬至 `src/task/exec_mart_sql.py`，DAG 只留串接 | 依原案 |
| 6 | `read_sql` → `exec_mart_sql_files`、`exec_sql_linebyline` → `exec_sql_multistatement` | 依原案 |
| 7 | `find_sql_files` 一併搬遷 | 依原案 |

### 實作中的一項調整：非 200 改以 `raise_for_status()` 表達

原案是「非 200 就 `raise`」，實作時發現更精確的做法是
`response.raise_for_status()` —— 它把非 2xx 轉成 `HTTPError`，
交給**既有的 `except requests.exceptions.HTTPError` 分支**分類後原樣拋出。

副作用是那個 `except` 分支終於成為活的：原實作從未呼叫 `raise_for_status()`，
`requests.get()` 也不會自動拋 `HTTPError`，所以該分支在本輪之前是死碼。

連帶地，`if soup is not None:` 這層判斷成為恆真（走到 `else` 時
`raise_for_status()` 已保證解析過），予以移除。**這一步是必要的**：
若保留該層而把 `return` 放在其外，`soup` 為 `None` 時又會回傳一個未寫檔的路徑，
病因原地復發。

---

## 三、實際改動

### 新增模組

```
src/task/exec_mart_sql.py          由 dags/d04 搬出的三支函式
    find_sql_files                 原樣搬遷（僅補 docstring）
    exec_sql_multistatement        原 exec_sql_linebyline，修好三個缺陷
    exec_mart_sql_files            原 read_sql，修好 finally 守衛
```

### 守衛抽成共用函式

```
src/util/mysql_utils.py
    新增 close_quietly(resource, resource_name)
    upsert_to_table 的 finally：10 行手寫守衛 → 2 行呼叫
```

「關閉資源，失敗只記 warning」原本要在四個 `finally` 裡各寫一次
（`upsert_to_table` 的 cursor 與 conn、`exec_mart_sql` 兩支函式）。
抽成一支三行的公開函式後，四處都縮為兩行呼叫。

放在 `mysql_utils` 是因為兩個呼叫端都在操作 MySQL 資源；
函式本身對任何具備 `close()` 的物件都成立。

### DAG 瘦身

```
dags/d04_analysis_pedestrian_accidents.py
    117 行 → 50 行
    移除 logger、mysql_utils 匯入；只保留 @task 串接與排程設定
    sqlparse 註解區塊隨 exec_sql_linebyline 一併移出
```

`d04` 現在與其他五支 DAG 結構一致：`@task` 包住 `src/task/` 的函式，DAG 只定義相依。

### 抓取層

```
src/task/e_crawling_nightmarket.py  find_tw_night_markets_list
    finally: return str(csvfile_name)   → 移除；return 移至 else 尾端
    if response.status_code == 200      → response.raise_for_status()
    if soup is not None:（恆真層）       → 移除
    if df.empty: logger.info(...)       → raise ValueError(...)
    soup = None（失去用途的宣告）        → 移除
    docstring 補上 :raises: 與回傳值的新契約
```

### 語意變化摘要

| 情境 | 改動前 | 改動後 |
| --- | --- | --- |
| 維基百科連線失敗 | 回傳不存在的檔案路徑 | 拋 `ConnectionError` |
| 回應非 2xx | 回傳不存在的檔案路徑 | 拋 `HTTPError` |
| 解析不到任何夜市 | 寫出只有標題列的 CSV 並回傳 | 拋 `ValueError` |
| mart SQL 執行失敗 | 例外被 `finally: return None` 吞掉 | 原樣拋出，事務已 `rollback` |
| mart SQL 連線失敗 | `UnboundLocalError` 取代真正的錯誤 | 原樣拋出 `OperationalError` |
| `close()` 自身失敗 | 取代正在傳播的例外 | 記 `warning`，原例外照常傳出 |

---

## 四、驗證方式

```bash
poetry run pytest test/unit_test/          # 71 passed（前一輪 52 → 本輪 71）
poetry run pre-commit run --files <改動檔案>  # ruff check / ruff format 皆 Passed
```

本輪新增 19 個測試：

**`test_task_exec_mart_sql.py`（12 個）**

| 測試 | 釘住的行為 |
| --- | --- |
| `test_找不到_sql_檔案時拋出` / `test_遞迴蒐集所有_sql_檔案` | `find_sql_files` 搬遷後的行為 |
| `test_執行失敗時復原事務並原樣拋出` | **判準 1**：例外必須穿透 `finally` |
| `test_關閉失敗不會取代正在傳播的例外` | **判準 2**：`close()` 拋例外時，傳出去的仍是原始的資料庫錯誤 |
| `test_連線建立失敗時不會變成_unboundlocalerror` | **判準 3** |
| `test_檔案讀取失敗時原樣拋出` | 非資料庫錯誤同樣不被吞 |
| `test_全數成功才提交一次` | 多檔案共用單一事務 |
| `test_消耗掉每個檔案的所有_result_set` | multistatement 的 `next_result()` 迴圈 |
| `test_未指定資料庫時取用環境變數` | DAG 端的預設路徑 |
| `test_單段_sql_*`（3 個） | 子決策 2 保留的零呼叫端函式 |

**`test_task_e_crawling_nightmarket.py`（5 個）**

| 測試 | 釘住的行為 |
| --- | --- |
| `test_連線失敗時拋出而非回傳路徑` | 病因本身 |
| `test_逾時的例外同樣不被吞掉` | 四個 `except` 分支都曾因 `finally: return` 失效 |
| `test_非_200_回應會拋出` | 子決策 3 |
| `test_解析不到任何夜市時拋出` | 子決策 3 的空 `df` 路徑 |
| `test_成功時回傳確實存在的檔案路徑` | 新契約：**實際讀回 CSV 驗證內容**，而非只檢查回傳字串 |

**`test_util_mysql_utils.py`（新增 2 個）**

| 測試 | 釘住的行為 |
| --- | --- |
| `test_close_quietly_吞掉關閉時的例外` | 共用守衛的核心行為 |
| `test_close_quietly_容許_none` | 連線建立失敗路徑（資源尚未存在） |

既有的 `test_util_logger_crtx.py` 的 ETL 模組數斷言 20 → 21（新增 `exec_mart_sql`）。

另以 AST 掃描全 repo 驗收兩件事：

```
d04 匯入名稱缺失: 無
finally 內的 return/break/continue: 0 處      ← src/ 與 dags/ 全掃
```

`d04` 無法在本機以 Airflow 實際解析（開發環境未安裝 airflow），
故以「AST 解析 `d04` 的 `from src.* import`，逐一確認名稱存在於目標模組」代替。

---

## 五、同病因但尚未修復的位置

候選 4 的病因（`finally` 職責越界）在 `src/` 與 `dags/` 內已無殘留 ——
AST 掃描顯示 `finally` 區塊內 0 處 `return` / `break` / `continue`。

### A. 本輪浮現、刻意未處理

| 位置 | 現象 | 歸屬 |
| --- | --- | --- |
| `e_crawling_nightmarket.py:57` | `response = None` 宣告後從未被引用（`try` 第一行即賦值，`except` 分支也不讀它） | 既存死碼，依「只碰必要之處」保留 |
| `search_place_id` / `get_place_details` | 四個 `except` 分支的結構與 `find_tw_night_markets_list` 相同，但沒有 `finally: return`；問題在於 HTTP 200 但語意失敗（`OVER_QUERY_LIMIT`）被當成「找不到」 | 候選 6 |
| `d04:44` | `Path().resolve() / "src/task/mart_table_sql"` 落點隨行程 CWD 漂移 | 候選 8 |

### B. 屬於其他候選（沿襲前一輪，狀態未變）

| 位置 | 現象 | 歸屬 |
| --- | --- | --- |
| `src/task/e_*.py` | 非 200 僅 `return None`，呼叫端 `.extend(None)` 觸發 `TypeError`；Google `OVER_QUERY_LIMIT`（HTTP 200）被當成「找不到地點」；無重試、無節流 | 候選 6 |
| 全 codebase | 設定散在多個 `load_dotenv()` 與模組層全域變數，零驗證 | 候選 7 |
| `src/task/e_*.py`、`t_fact_night_markets.py`、`dags/d04` | `Path().resolve()` 使落點隨行程 CWD 漂移 | 候選 8 |
| `cal_accidents_nearby_nightmarket` | 一夜市一次 round-trip 的 N+1 查詢；查詢字串以 f-string 內插經緯度 | 候選 10 |
| `src/task/t_*.py` 8 支 | CSV 讀取骨架重複（缺病因的另一半：零 try/except、零資源管理） | 待評估 |

### 判定不做：把整套連線樣板抽成共用介面

`upsert_to_table`、`exec_mart_sql_files`、`exec_sql_multistatement` 三支函式
骨架相同，但 `except` 分層與日誌內容全部不同（一支分兩層例外、一支要帶迴圈中
失敗的檔名、一支是固定字串）。要共用就得把例外分類策略與訊息組裝變成參數，
產生一個假共用點。

**本輪明確判定不做，且不列為候選 1 的待辦**，理由與判準記於
[ADR-0005](./0005-finally-只負責釋放資源.md)的「明確不做」章節。
`close_quietly()` 能抽出來是因為它沒有這種分歧 —— 共用點該落在沒有差異的地方。

**候選 6 與本輪的關聯**：子決策 3 讓 `find_tw_night_markets_list` 的非 200
從「靜默」變成「失敗」，但**沒有加上重試或退避**。維基百科偶發 5xx 現在會直接
讓 `d05` 紅燈（DAG 層設定了 `retries: 3`，實務上由 Airflow 重試吸收）。
候選 6 應處理函式層級的重試與節流。

### C. 既存 lint 債（狀態未變）

`ruff check src dags` 仍未通過的檔案 7 個（29 個錯誤），清單見
[ADR-0003 執行摘要](./0003-執行摘要-快取層與日誌職責.md)第五章 C。
本輪改動的三個檔案皆已通過。

---

## 六、`dags/` 現況

搬遷後六支 DAG 的結構終於一致 —— 全部只做「`@task` 包住 `src/` 的函式 + 定義相依」：

| DAG | 呼叫的模組 |
| --- | --- |
| `d01_create_calendar_this_year` | `create_traffic_accident_tables`、`t_dim_accident_day`、`l_dim_accident_day`、`mysql_utils` |
| `d02` / `d03_track_*_traffic_accidents` | `e_/t_/l_` 交通事故系列 |
| `d04_analysis_pedestrian_accidents` | **`exec_mart_sql`（本輪新增）** |
| `d05_track_night_markets` | `create_night_markets_tables`、`e_crawling_nightmarket`、`t_fact_night_markets`、`mysql_utils` |
| `d06_precompute_to_redis` | `c_data_service` |

`dags/` 底下已無業務邏輯，`src/task/` 的 21 個模組全部可在無 Airflow 的環境匯入
（由 `test_util_logger_crtx.py` 把關）。

---

## 七、建議的下一步

1. **候選 6**（抓取層重試、節流與狀態碼語意）—— 本輪把非 200 從靜默改為失敗，
   但沒有重試；且 Google API 的 `OVER_QUERY_LIMIT`（HTTP 200）仍被當成「找不到」。
   同時可解 `crawling_utils.download_and_extract_zip()` 接上的阻礙
2. **候選 8**（`Path().resolve()` 的 CWD 漂移）—— 範圍明確，且與候選 6 同樣落在 `e_*.py`
3. **候選 7**（設定分散）—— 影響面最廣，但也最需要先想清楚介面
4. **候選 10**（預計算的查詢粒度）—— 效能收益大但需驗證輸出等價
5. 確認 `convert_time_zone.py` / `validate_csv_encoding.py` 兩個孤兒模組是否可刪
