# ADR-0004：MySQL Engine 改為以資料庫名為鍵的單例快取

- 日期：2026-07-30
- 狀態：已採納
- 範圍：`src/util/mysql_utils.py`、`src/util/get_table_from_sql_server.py`、
  `src/util/redis_utils.py`（僅命名對齊）、
  `src/task/t_fact_accident_main.py`、`src/task/t_fact_accident_env.py`、
  `src/task/t_fact_accident_human.py`、`src/task/core/c_db.py`、
  `dags/d01_create_calendar_this_year.py`、`dags/d05_track_night_markets.py`
- 相關：[ADR-0001 不使用 AirflowException](./0001-不使用-airflowexception-一律原樣拋出.md)、
  [ADR-0002 upsert 邏輯集中於單一 module](./0002-upsert-邏輯集中於單一-module.md)、
  [ADR-0003 快取層不再吞噬例外](./0003-快取層不再吞噬例外.md)
- 來源：[ADR-0003 執行摘要](./0003-執行摘要-快取層與日誌職責.md)第五章 B 提出的「候選 9」

## 背景

`create_engine_to_mysql()` 每次呼叫都建立一個全新的 SQLAlchemy `Engine`：

```python
def create_engine_to_mysql(database: str | None = None) -> Engine:
    ...
    engine = create_engine(
        connection_url,
        pool_size=5,
        pool_recycle=3600,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 120},
    )
    return engine
```

呼叫端一律是「建 Engine → 開一條連線 → 用完 → 丟掉 Engine」：

```python
def get_table_from_sqlserver(dql_str, params=None, *, database=None):
    engine = create_engine_to_mysql(database)   # 每次呼叫都建一個新 Engine
    with engine.connect() as conn:
        ...
    return df                                    # 從不 dispose
```

### 1. 三個池參數形同虛設

`Engine` 攜帶的是一個連線池。上面的用法會開一條連線、用完歸還**自己那個池**，
然後整個池連同池裡的連線一起被丟棄。**池永遠不會服務第二個請求**，
於是 `pool_size=5`（池容量）、`pool_recycle=3600`（連線回收週期）、
`pool_pre_ping=True`（取用前探活）三個參數全部沒有作用的機會 ——
它們描述的是一個從未被重複使用的池。

### 2. Engine 從不 `dispose()`

沒有任何呼叫端呼叫 `engine.dispose()`，連線的釋放完全依賴 GC 的非確定性回收。
在 CPython 的引用計數下多半會即時回收，但這是實作細節而非契約。

### 3. 熱路徑代價高

`c_data_service.cal_accidents_nearby_nightmarket()` 在
`for a_nightmarket in batch` 迴圈內逐個夜市查詢，每次查詢都會走一遍
`get_table_from_sqlserver` → `create_engine_to_mysql`。以 300+ 個夜市計，
`d06` 每輪約建立 **300 個 Engine**（＝300 個池、300 次 TCP 握手與 MySQL 認證）。

`get_table_from_sqlserver` 全 repo 共 16 個呼叫點，另有 `d01`、`d05` 直接呼叫。

### 4. 同一 repo 內已有正解

`redis_utils._get_redis_pool()` 以模組層單例解決了**完全相同**的問題：

```python
_REDIS_POOL = None

def _get_redis_pool() -> redis.ConnectionPool:
    global _REDIS_POOL
    if _REDIS_POOL is None:
        _REDIS_POOL = redis.ConnectionPool(...)
    return _REDIS_POOL
```

Redis 有做，MySQL 沒有。本 ADR 補上這個不對稱。

（原始碼中該變數名為 `REDIS_POOL`，本輪一併改為 `_REDIS_POOL`，見子決策 9。）

---

## 決策

**`mysql_utils` 維護一個以資料庫名為鍵的模組層 Engine 快取；同一個 `database`
在同一行程內只會有一個 Engine。**

```python
_ENGINES: dict[str | None, Engine] = {}

def get_engine_to_mysql(database: str | None = None) -> Engine:
    if database not in _ENGINES:
        logger.info(f"==== Creating SQLAlchemy Engine for database `{database}` ====")
        _ENGINES[database] = _create_engine(database)
    return _ENGINES[database]
```

### 子決策

| # | 決策 | 理由 |
| --- | --- | --- |
| 1 | 範圍**只含 SQLAlchemy Engine**；`get_pymysql_conn_to_mysql` 與 `get_pymysql_conn_to_mysql_multistatement` 兩支裸連線不動 | 裸連線本來就有明確的 `close()`（見 `upsert_to_table` 的 `finally`），不存在「池被丟棄」的病因。混進來會讓本 ADR 的病因定義失焦 |
| 2 | 快取鍵是 `database`（含 `None`），型別 `dict[str \| None, Engine]` | `host` / `port` / `username` / `password` 是模組層全域，行程內不會變；`database` 是唯一真的會變的維度（`d01` 建庫時傳 `None`）。以 `(host, port, database)` 為鍵是為零需求防禦 |
| 3 | 池參數**全數沿用現值**，不調整也不新增 `max_overflow` | 本輪病因是「池被丟棄」，不是「池太小」。單例化之後這些參數才第一次真的生效，該不該調要有實測數據再說 |
| 4 | 公開介面改名為 `get_engine_to_mysql`；實際建立退為私有 `_create_engine` | 單例化後 `create_` 這個動詞會說謊，讓維護者以為每次拿到新 Engine 而寫出錯誤的 `dispose()` 呼叫。`get_` 對齊 `redis_utils._get_redis_pool` 的語意 |
| 5 | **不提供公開的 `dispose_all_engines()`** | 沒有任何生產程式碼需要它。測試的隔離需求由測試層自行 `_ENGINES.clear()` 解決，不為測試在生產介面開洞 |
| 6 | 僅在快取未命中（真的建立 Engine）時 `logger.info`，命中時靜默 | log 本身就成為「池只建一次」的現場證據：`d06` 跑完後 log 裡只會有一行 |
| 7 | **不加 `try/except`** | `create_engine()` 是惰性的 —— 只組 URL、不連線，實質不會拋例外。真正的連線錯誤發生在 `engine.connect()`，由呼叫端處理。為不可能情境寫錯誤處理只是雜訊 |
| 8 | `get_table_from_sqlserver` **併入 `mysql_utils`**，函式名不改；`get_table_from_sql_server.py` 瘦成一行 re-export shim | 該模組存在的唯一理由就是「包一層 Engine 建立 + 查詢」，而本輪正好改寫這層。併入後 `mysql_utils` 成為唯一的 MySQL 存取面（連線建立與查詢同居一處）。函式名維持不變，避免改動面擴散到 16 個呼叫點 |
| 9 | `redis_utils.REDIS_POOL` 一併改名為 `_REDIS_POOL` | 既然本 ADR 以 Redis 的單例為範本，兩支模組層快取變數的可見性標示就該一致 —— 兩者都是「外部不該直接碰」的狀態，外部應走 `create_redis_client()` / `get_engine_to_mysql()`。`_REDIS_POOL` 也與同檔既有的私有函式 `_get_redis_pool()` 對齊。零外部引用，改動僅 5 行單檔 |

單複數的差異（`_REDIS_POOL` 單數、`_ENGINES` 複數）刻意保留：Redis 只有一個池，
MySQL 每個資料庫一個，名稱應反映資料結構而非強求對稱。

### 為什麼是模組層字典，而不是 `lru_cache`

`functools.lru_cache` 可以達到同樣效果且更短，但：

- 快取的清空只能靠 `.cache_clear()`，測試中不易與「只有這個 database 要重建」的
  情境對齊；
- `lru_cache` 的預設 `maxsize=128` 會在超出時**默默丟棄 Engine**（連同未關閉的池），
  正是本 ADR 要消滅的行為；
- 顯式的 `dict` 讓「快取的生命週期是整個行程」這件事直接寫在程式碼裡。

### 執行環境的安全性

- **Airflow（LocalExecutor）**：三個 Engine 建立點都在 `@task` 函式體內，
  不在 DAG parse 階段。每個 task 在自己的子行程執行、各自持有一份 `_ENGINES`，
  不會發生「fork 繼承已開啟連線」的經典問題。
- **Streamlit（Cloud Run）**：長駐行程、多 script thread 共用同一個 Engine。
  SQLAlchemy 的 `QueuePool` 本身是 thread-safe 的，這正是它被設計出來的用途。

---

## 後果

- `d06` 每輪的 Engine 數從約 300 降為 1；連線建立成本從「每次查詢一次握手」
  降為「整個 task 行程一次」。
- `pool_size` / `pool_recycle` / `pool_pre_ping` 首次真的生效。
- `mysql_utils` 成為 MySQL 存取的單一入口，`get_table_from_sql_server.py`
  退化為待刪除的 shim。
- 行程內的 MySQL 連線上限變成可推理的值（`pool_size=5` + SQLAlchemy 預設
  `max_overflow=10`，即每行程最多 15 條），而非「與同時查詢次數成正比」。

### 誠實的限制

- **Engine 仍然不會被 `dispose()`** —— 只是從「每次呼叫都洩漏一個池」變成
  「整個行程持有一個池，行程結束時由 OS 回收」。這是刻意的：
  行程等長的生命週期不需要顯式釋放，加上 `atexit` 之類的鉤子反而增加失敗模式。
- **`_ENGINES` 是可變的模組層全域狀態**。它與 `redis_utils._REDIS_POOL` 有相同的
  測試汙染風險，靠測試層的 autouse fixture 清空來管理，而非型別系統。
- 若未來 `MYSQL_HOST` 在單一行程內需要變動（例如讀寫分離），
  以 `database` 為鍵就不夠了。那屬於**候選 7（設定分散）**的範疇，
  屆時應連同設定集中化一起處理，而不是先在此埋一個推測性的複合鍵。

## 未納入本次範圍

- **迴圈內的 N+1 查詢**：`cal_accidents_nearby_nightmarket` 仍是一個夜市一次
  round-trip。Engine 快取消掉了「300 個池」，但沒有消掉「300 次查詢」。
  這是**查詢粒度**的病因，與本輪的**資源生命週期**不同，且改寫成批次查詢會動到
  聚合邏輯與計算結果，風險遠高於快取單例。另開候選處理。
- **`get_table_from_sqlserver` 這個誤導性的函式名**（查的是 MySQL 不是 SQL Server）：
  改名要動 16 個呼叫點，與本輪病因無關。
- **`get_table_from_sql_server.py` 的實際刪除**：本輪只瘦成 shim，
  待人工審閱確認零呼叫端後再刪，沿用 ADR-0001 決策 5 的作法。
- **`upsert_to_table` 的 pymysql 裸連線**：見子決策 1。
