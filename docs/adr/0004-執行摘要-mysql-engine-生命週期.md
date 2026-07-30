# 執行摘要：MySQL Engine 生命週期與單一存取面

- 日期：2026-07-30
- 分支：`refactor/util-connection-layer`
- 依據：[ADR-0004 MySQL Engine 以資料庫名為鍵的單例快取](./0004-mysql-engine-以資料庫名為鍵的單例快取.md)
- 前一輪：[ADR-0003 執行摘要](./0003-執行摘要-快取層與日誌職責.md)
- 性質：**interim execution report**

| 候選 | 標題 | 狀態 |
| --- | --- | --- |
| 候選 9 | 資料庫連線資源生命週期 | 已完成 |

候選 9 由前一輪執行摘要第五章 B 提出，第七章列為下一步第 1 項。

**範圍擴張**：本輪另含三項超出 ADR-0004 原範圍的工作，皆因結構性關聯而併入：

1. **移除 `create_database` / `create_tables` 的 `finally: engine.dispose()`** ——
   單例化的直接後果，不移除即與本 ADR 的決策相矛盾。
2. **`get_table_from_sqlserver` 併入 `mysql_utils`，舊檔刪除** —— 該模組存在的唯一
   理由就是本輪要改寫的那一層。
3. **`redis_utils.REDIS_POOL` 改名 `_REDIS_POOL`** —— 本 ADR 以 Redis 的單例為範本，
   兩者的可見性標示應一致。

---

## 一、病因定義

候選 9 的病因是：

> **每次取用資源都新建一個「池」，用完連同池一起丟棄，
> 使池的設定參數永遠沒有生效的機會。**

三個可檢驗的判準：

1. **每次呼叫都建立** —— 建構函式直接出現在熱路徑上，無快取
2. **設定形同虛設** —— 池的容量、回收、探活參數描述的是一個從未被重複使用的池
3. **從不釋放** —— 沒有任何呼叫端負責 `dispose()`，只能等 GC

實測掃描的結果：

| 位置 | 現象 |
| --- | --- |
| `create_engine_to_mysql` | 每次呼叫建立全新 Engine（＝全新連線池） |
| `get_table_from_sqlserver` | 每次查詢呼叫一次上者，全 repo 16 個查詢點 |
| `cal_accidents_nearby_nightmarket:360` | 位於 `for a_nightmarket in batch` 迴圈內 |
| `create_database` / `create_tables` | `finally: engine.dispose()` —— 舊模型下正確，單例化後會關掉共用池 |

淨結果：**`d06` 每輪約建立 300 個 Engine、300 個池、300 次 TCP 握手與 MySQL 認證，
而 `pool_size=5` / `pool_recycle=3600` / `pool_pre_ping=True` 三個參數從未生效過。**

同一 repo 內已有正解：`redis_utils._get_redis_pool()` 以模組層單例解決了完全相同的
問題。Redis 有做，MySQL 沒有 —— 本輪補上這個不對稱。

---

## 二、決策摘要

| # | 決策 | 狀態 |
| --- | --- | --- |
| 1 | 範圍只含 SQLAlchemy Engine，pymysql 裸連線不動 | 依原案 |
| 2 | 快取鍵為 `database`（含 `None`），型別 `dict[str \| None, Engine]` | 依原案 |
| 3 | 池參數全數沿用現值，不調整也不新增 `max_overflow` | 依原案 |
| 4 | 公開介面改名 `get_engine_to_mysql`，建立退為私有 `_create_engine` | 依原案 |
| 5 | 不提供公開的 `dispose_all_engines()` | 依原案 |
| 6 | 僅快取未命中時 `logger.info` | 依原案 |
| 7 | 不加 `try/except`（`create_engine()` 惰性，不連線） | 依原案 |
| 8 | `get_table_from_sqlserver` 併入 `mysql_utils`，函式名不改 | 依原案 |
| 9 | **`REDIS_POOL` → `_REDIS_POOL`** | 實作中新增 |

### 決策 9 的來由

實作完成後，兩支模組層快取變數並排比較，可見性標示不一致：

| | 底線前綴 | 大小寫 | 單複數 |
| --- | --- | --- | --- |
| `REDIS_POOL` | 無 | 全大寫 | 單數 |
| `_ENGINES` | 有 | 全大寫 | 複數 |

兩者都是「外部不該直接碰」的狀態（對外入口是 `create_redis_client()` 與
`get_engine_to_mysql()`），故統一加上底線前綴；`_REDIS_POOL` 也與同檔既有的
私有函式 `_get_redis_pool()` 對齊。

**單複數的差異刻意保留** —— Redis 只有一個池，MySQL 每個資料庫一個，
名稱應反映資料結構而非強求對稱。

### 動工中浮現的衝突：`finally: engine.dispose()`

`create_database:198` 與 `create_tables:239` 各有一個 `finally: engine.dispose()`。
在「每次都新建 Engine」的舊模型下這是正確的資源釋放；單例化後，Engine 的所有權
移交給 `mysql_utils`，呼叫端把共用的池關掉正是本輪要消滅的行為。

不是硬性 crash（SQLAlchemy 的 Engine 在 `dispose()` 後仍可用，只是重建一個新池），
但 `d01` / `d05` 每次執行都會白白丟棄一個池。兩處一併移除。

---

## 三、實際改動

### 連線層（ADR-0004 原範圍）

```
src/util/mysql_utils.py
    create_engine_to_mysql        → 私有 _create_engine（實作邏輯逐字不變）
    新增 _ENGINES: dict[str | None, Engine]
    新增 get_engine_to_mysql      → 快取未命中才建立並記錄一行 logger.info
    新增 get_table_from_sqlserver → 由舊模組併入，函式名與簽章不變
    create_database               → 移除 finally: engine.dispose()
    create_tables                 → 移除 finally: engine.dispose()

src/util/redis_utils.py           REDIS_POOL → _REDIS_POOL（5 處，零外部引用）

src/util/get_table_from_sql_server.py   已刪除
```

### 呼叫端

```
src/task/t_fact_accident_main.py   ─┐
src/task/t_fact_accident_env.py     ├ 匯入來源改為 src.util.mysql_utils
src/task/t_fact_accident_human.py   │  （函式名不變，僅一行 import 改動）
src/task/core/c_db.py              ─┘

dags/d01_create_calendar_this_year.py   create_engine_to_mysql → get_engine_to_mysql
dags/d05_track_night_markets.py         同上
```

### 成果

| 指標 | 改動前 | 改動後 |
| --- | --- | --- |
| `d06` 每輪建立的 Engine 數 | 約 300 | 1 |
| `d06` 每輪的 MySQL 認證次數 | 約 300 | 1（後續由池提供連線） |
| `pool_size` / `pool_recycle` / `pool_pre_ping` | 從未生效 | 生效 |
| 行程內 MySQL 連線上限 | 與同時查詢次數成正比 | 15（`pool_size=5` + 預設 `max_overflow=10`） |
| MySQL 存取的入口模組數 | 2（`mysql_utils` + `get_table_from_sql_server`） | 1 |

`d06` 執行後的 log 中，`==== Creating SQLAlchemy Engine for database ... ====`
只會出現一行 —— 這是決策 6 刻意設計的現場證據。

---

## 四、驗證方式

```bash
poetry run pytest test/unit_test/          # 52 passed（前一輪 44 → 本輪 52）
poetry run pre-commit run --files <改動檔案>  # ruff check / ruff format 皆 Passed
```

本輪新增 8 個測試（`test_util_mysql_engine_cache.py`），以 autouse fixture
清空 `_ENGINES` 達成測試間隔離（決策 5：不為測試在生產介面開洞）：

| 測試 | 釘住的行為 |
| --- | --- |
| `test_同一資料庫重複呼叫回傳同一個_engine` | 決策 1 的核心 |
| `test_不同資料庫各自持有獨立的_engine` | 決策 2 的快取鍵 |
| `test_未指定資料庫的_none_也會被快取` | `d01` 建庫路徑（`database=None`）不被漏掉 |
| `test_快取命中時不再呼叫_create_engine` | 直接釘住「只建立一次」，而非僅比對物件同一性 |
| `test_多次查詢只建立一個_engine` | 熱路徑保證：300 次查詢 → 1 個 Engine、300 次 `connect()` |
| `test_建表函式不再_dispose_共用的_engine` | 動工中浮現的衝突 |
| `test_舊版_create_engine_to_mysql_已不存在` | 不留誤導性的別名 |
| `test_舊模組已刪除` | 不留第二份實作 |

既有的 `test_task_create_tables.py` 有三處 `engine.dispose.assert_called_once()`
釘住舊行為，隨決策改為 `assert_not_called()`，其中一個測試名與 docstring
同步指向 ADR-0004。

另以 grep 確認全 repo（排除 `__pycache__`）中 `create_engine_to_mysql` 與
`get_table_from_sql_server` 零殘留，以及沿用前三輪的匯入健檢：
`src/` 底下 34 個模組全部可獨立匯入。

---

## 五、同病因但尚未修復的位置

候選 9 的病因（每次取用都新建池、設定失效）在 `src/` 內已無殘留。

### A. 刻意未納入

| 位置 | 現象 | 理由 |
| --- | --- | --- |
| `get_pymysql_conn_to_mysql` | 每次呼叫建立新的裸連線 | 有明確的 `close()`（見 `upsert_to_table` 的 `finally`），不存在「池被丟棄」的病因。見 ADR-0004 決策 1 |
| `get_pymysql_conn_to_mysql_multistatement` | 同上 | 同上；且僅 `d04` 使用 |
| Engine 仍不 `dispose()` | 行程結束時由 OS 回收 | 行程等長的生命週期不需顯式釋放，加 `atexit` 鉤子只增加失敗模式。見 ADR-0004「誠實的限制」 |

### B. 建議新開「候選 10：預計算的查詢粒度」

Engine 快取消掉了「300 個池」，但沒有消掉「300 次查詢」。

```python
for a_nightmarket in batch:            # 300+ 個夜市
    ...
    df_nearby_accidents = get_accident_table_pedestrian_involved_in(query)
```

`cal_accidents_nearby_nightmarket` 仍是一個夜市一次 round-trip，
每次都是一段獨立的經緯度矩形範圍查詢。這是典型的 N+1：
可改為單次查詢（矩形範圍以 `OR` 串接或改用空間索引），再於記憶體中分派回各夜市。

**歸類理由**：候選 9 的病因是「資源生命週期」，本項是**查詢粒度**。
兩者在同一段程式碼上顯現，但修法與風險完全不同 ——
批次查詢會動到聚合邏輯與計算結果，需要對照修改前後的輸出，
遠不是「加一層快取」那種可以靠型別與單測保證的改動。

同一段程式碼另有一處值得順帶檢視：查詢字串以 f-string 內插經緯度組成
（`WHERE latitude BETWEEN {params["min_lat"]} ...`），而非交給 bind parameter，
與前一輪執行摘要指出的 `c_db.py:38-40` 屬同型問題。

### C. 屬於其他候選（沿襲前一輪，狀態未變）

| 位置 | 現象 | 歸屬 |
| --- | --- | --- |
| `dags/d04` `exec_sql_linebyline()` / `read_sql()` | `finally` 內 `return None` 吞例外；連線建立於 `try` 之外 | 候選 4 |
| `src/task/e_crawling_nightmarket.py` `find_tw_night_markets_list()` | `finally: return` 吞掉四個 `except` 剛拋出的例外 | 候選 4 |
| `src/task/e_*.py` | 非 200 僅 `return None`；Google `OVER_QUERY_LIMIT`（HTTP 200）被當成「找不到地點」；無重試、無節流 | 候選 6 |
| 全 codebase | 設定散在多個 `load_dotenv()` 與模組層全域變數，零驗證 | 候選 7 |
| `src/task/e_*.py`、`t_fact_night_markets.py`、`dags/d04` | `Path().resolve()` 使落點隨行程 CWD 漂移 | 候選 8 |
| `src/task/t_*.py` 8 支 | CSV 讀取骨架重複（缺病因的另一半：零 try/except、零資源管理） | 待評估 |

**候選 7 與本輪的關聯**：`_ENGINES` 以 `database` 為鍵的前提是
`MYSQL_HOST` / `MYSQL_PORT` 在單一行程內不變。若未來需要讀寫分離或多主機，
應連同設定集中化一起處理，而不是先在此埋一個推測性的複合鍵
（見 ADR-0004「誠實的限制」）。

### D. 既存 lint 債（狀態未變）

`ruff check src dags` 仍未通過的檔案 7 個（29 個錯誤），清單見
[ADR-0003 執行摘要](./0003-執行摘要-快取層與日誌職責.md)第五章 C。
依「只碰必要之處」原則，本輪僅處理實際改動的檔案。

---

## 六、`src/util/` 現況

```
logger_crtx.py          ADR-0001 的成果，全 codebase 共用
mysql_utils.py          MySQL 的唯一存取面：Engine 快取、綱要檢查、查詢、upsert
redis_utils.py          Redis 連線池單例與 Pickle 快取讀寫
crawling_utils.py       尚未接上（見下）
table_column_map.py     中→英欄位對照
convert_time_zone.py    零呼叫端的孤兒模組
validate_csv_encoding.py 零呼叫端的孤兒模組
```

`mysql_utils` 在本輪之後成為 MySQL 存取的單一入口 ——
連線建立、綱要檢查、查詢、寫入四件事同居一處。

未變的兩項阻礙（沿襲前一輪）：

- `crawling_utils.download_and_extract_zip()` 的參數順序與現有呼叫端不相容，
  仍是它接上 `e_crawling_traffic_accident.py` 的阻礙（與候選 6 同時可解）。
- `convert_time_zone.py` / `validate_csv_encoding.py` 兩個孤兒模組去留未定。

---

## 七、建議的下一步

1. **候選 4**（`finally: return` 吞例外）—— 三處，其中兩處在 `d04`；
   病因與 ADR-0003 同源，修法已有前例
2. **候選 6**（抓取層重試與狀態檢查）—— 同時可解 `crawling_utils` 接上的阻礙
3. **候選 10**（預計算的查詢粒度）—— 本輪新提出；效能收益大但需驗證輸出等價
4. 確認 `convert_time_zone.py` / `validate_csv_encoding.py` 兩個孤兒模組是否可刪
