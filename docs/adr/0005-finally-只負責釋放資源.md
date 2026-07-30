# ADR-0005：`finally` 只負責釋放資源，不決定回傳值

- 日期：2026-07-30
- 狀態：已採納
- 範圍：`src/task/e_crawling_nightmarket.py`、`dags/d04_analysis_pedestrian_accidents.py`、
  新增 `src/task/exec_mart_sql.py`
- 相關：[ADR-0001 不使用 AirflowException](./0001-不使用-airflowexception-一律原樣拋出.md)、
  [ADR-0003 快取層不再吞噬例外](./0003-快取層不再吞噬例外.md)、
  [ADR-0004 MySQL Engine 單例快取](./0004-mysql-engine-以資料庫名為鍵的單例快取.md)
- 來源：[ADR-0003 執行摘要](./0003-執行摘要-快取層與日誌職責.md)第五章 A 提出的「候選 4」

## 背景

ADR-0003 消滅了「`except` 之後回傳空值」這種吞噬形態。候選 4 是它的**孿生病灶** ——
例外沒有被 `except` 吞掉（`except` 區塊確實寫了 `raise`），卻在 `finally` 被攔截。

### 1. `finally: return` 讓四個 `raise` 全部失效

`e_crawling_nightmarket.find_tw_night_markets_list()`：

```python
try:
    response = requests.get(url, headers=headers, timeout=120)
    ...
except requests.exceptions.Timeout:
    logger.error(f"Timeout while fetching from {url}")
    raise                      # ← 這四個 raise
except requests.exceptions.ConnectionError:
    ...
    raise
except requests.exceptions.HTTPError:
    ...
    raise
except Exception:
    ...
    raise
else:
    ...
finally:
    return str(csvfile_name)   # ← 全部被這一行取消
```

Python 的語意是：`finally` 區塊中的 `return` 會**丟棄**正在傳播的例外。
四個 `except` 分支各自精確分類了錯誤、記了日誌、寫了 `raise` ——
但沒有任何一個能真的傳出去。呼叫端拿到的永遠是一個字串路徑。

更糟的是這個路徑**指向一個不存在的檔案**：連線失敗時 `to_csv()` 從未執行。
`d05` 的下一個 task 會拿著它去開檔。

### 2. 同一支函式還有兩條「沒寫檔卻回傳路徑」的路徑

即使不談例外：

- `response.status_code != 200` 時沒有 `else`，`soup` 保持 `None`，
  跳過整個解析與寫檔，但 `finally` 照樣回傳路徑；
- `df.empty` 時只 `logger.info` 一行，然後仍然 `to_csv()` 出一個只有標題列的檔案。

維基百科改版導致解析不到任何表格，會安靜地產出空 CSV 並讓流程繼續。
這與 ADR-0003 判定過的病因（「無資料」與「故障」語意混同）是同一個。

### 3. `d04` 的 `finally: return None` 與 `UnboundLocalError`

`dags/d04_analysis_pedestrian_accidents.py` 的 `exec_sql_linebyline()`：

```python
try:
    conn = get_pymysql_conn_to_mysql_multistatement(database)   # ← 在 try 內綁定
    cursor = conn.cursor()
    cursor.execute(sql_str)
except Exception:
    logger.error("SQL執行失敗")
    if conn:                    # ← 連線失敗時 conn 未綁定 → UnboundLocalError
        conn.rollback()
    raise
finally:
    cursor.close()              # ← 同樣未綁定
    conn.close()
    return None                 # ← 即使沒有 UnboundLocalError，例外也在此被丟棄
```

三個缺陷疊在同一段：`finally` 吞例外、資源變數在失敗路徑上未綁定、
清理動作無守衛。連線失敗時真正的 `OperationalError`
會先被 `UnboundLocalError` 取代，再被 `return None` 吞掉。

### 4. `read_sql` 的 `finally` 會取代正在傳播的例外

```python
finally:
    cursor.close()
    conn.close()
```

`close()` 本身可能拋例外（pymysql 在連線已斷時會）。此時它會**取代**
`except` 剛剛 `raise` 出來的原始錯誤 —— 日誌上留下的是「關閉失敗」，
真正的原因消失。`mysql_utils.upsert_to_table` 已針對同一問題加了守衛，
`d04` 沒有跟上。

### 5. 結構問題：`d04` 的邏輯寫在 DAG 檔內，無法測試

`find_sql_files`、`exec_sql_linebyline`、`read_sql` 三支函式都定義在
`analysis_pedestrian_accidents()` 這個 `@dag` 函式**體內**，外部無法 import。
上述修正在現有結構下沒有任何測試能釘住。

這也違反專案自己的慣例（CLAUDE.md）：「`src/task/` 下每個檔案是一個 ETL 階段的
純函式……`dags/dNN_*.py` 只負責串接」。`d04` 是六支 DAG 中唯一的例外。

---

## 決策

**`finally` 只負責釋放資源。它不決定回傳值，也不製造新的例外。**

回傳值由正常路徑（`else` 或函式尾端）決定；例外由 `except` 分類、記錄、原樣拋出；
`finally` 內的每個清理動作都必須有守衛，確保它不會取代正在傳播的例外。

### 七項子決策

| # | 決策 | 理由 |
| --- | --- | --- |
| 1 | 三處全數納入本輪：`find_tw_night_markets_list`、`exec_sql_linebyline`、`read_sql` | 同一病因的三個標本，其中 `d04` 兩處相鄰。分批改要把同一段程式碼讀兩次 |
| 2 | `exec_sql_linebyline` **修好但不接回呼叫鏈** | 目前零呼叫端（唯一呼叫點在 `read_sql` 內已註解），但保留它作為 multistatement 單句執行的備用路徑。修好的成本低於留一個錯誤範例在 repo 裡 |
| 3 | `find_tw_night_markets_list` 的 `return` 移至 `else` 尾端；**非 200 與空 `df` 皆 `raise`** | 移除 `finally: return` 後，「什麼情況算真的產出了檔案」必須被明確回答。回傳值的意義嚴格收緊為「這個檔案存在且有內容」—— 這是移除 `finally: return` 的必要條件，不是額外的範圍 |
| 4 | 守衛抽成 `mysql_utils.close_quietly()`，由 `upsert_to_table` 與 `exec_mart_sql` 兩支共用；`conn` / `cursor` 初始化為 `None` 並在 `try` 內綁定 | 原案是「比照 `upsert_to_table` 各自手寫」，實作後發現同一段守衛會在 repo 內出現四次。抽成一支三行的函式即可消除。但**只抽這一層** —— 再往上抽的判定見下方「明確不做」章節 |
| 5 | `d04` 的三支函式**搬到 `src/task/exec_mart_sql.py`**，DAG 只留 `@task` 串接 | 本輪修正能被測試釘住的**前提**。同時讓 `d04` 回到專案既定架構 |
| 6 | 函式重新命名：`read_sql` → `exec_mart_sql_files`、`exec_sql_linebyline` → `exec_sql_multistatement`；`find_sql_files` 維持原名 | 兩個舊名都與實際行為不符 —— `read_sql` 會執行 SQL 並提交事務，`exec_sql_linebyline` 跑的是 multistatement 而非逐行。搬遷是改名的時機，之後再改要動兩個地方 |
| 7 | `find_sql_files` 一併搬遷 | 搬完之後 `d04` 是一支純粹的串接 DAG，與其他五支一致；且它的 `raise FileNotFoundError` 也才能被測試釘住。只有 8 行，搬遷成本低 |

### 為什麼不是「禁止使用 `finally`」

`finally` 本身沒有問題，它是唯一能保證清理一定發生的機制。
問題出在**放進去的東西**。本 ADR 給的是內容的判準，不是語法的禁令：

| 放在 `finally` 裡 | 判定 |
| --- | --- |
| `close()` / `release()`，且有守衛 | 正確用法 |
| `return` | 禁止 —— 會丟棄正在傳播的例外 |
| `break` / `continue` | 禁止 —— 同上 |
| 裸的清理呼叫（無守衛） | 需加守衛 —— 它可能取代原例外 |
| 業務邏輯（寫檔、送通知） | 不屬於這裡 —— 移到 `else` 或呼叫端 |

---

## 後果

- `d05` 的抓取 task 在維基百科不可達、回應非 200、或解析不到任何夜市時
  **會真的失敗**，而不是把一個空的或不存在的 CSV 路徑往下游傳。
- `d04` 的 SQL 執行失敗時，Airflow task log 裡留下的是真正的資料庫錯誤，
  而不是 `UnboundLocalError` 或什麼都沒有。
- `d04` 的三支函式成為可測試的模組層函式，`dags/d04` 縮減為串接。
- 全 codebase 的 `finally` 區塊只剩清理動作。

### 誠實的限制

- 子決策 3 讓 `find_tw_night_markets_list` 在維基百科頁面改版時**硬失敗**。
  這是刻意的（安靜地產出空 CSV 更糟），但代價是頁面結構的任何變動都會讓
  `d05` 紅燈，需要人工介入。若日後證實維基頁面結構經常小幅變動，
  應改為「解析到的夜市數低於某個門檻才失敗」，而不是退回無條件回傳。
- 子決策 2 保留了一支零呼叫端的函式。它不會被測試以外的任何程式碼執行，
  因此「修好了」這件事只有單元測試層級的保證。
- `close_quietly()` 放在 `mysql_utils` 是因為兩個呼叫端都在操作 MySQL 資源，
  但它本身對任何具備 `close()` 的物件都成立。若日後 Redis 或檔案處理也需要，
  應把它移到更中性的位置，而不是在 `redis_utils` 再抄一份。

## 未納入本次範圍

- **`Path().resolve()` 的 CWD 漂移**：`d04:113` 的
  `Path().resolve() / "src/task/mart_table_sql"` 使 SQL 檔的尋找位置隨行程工作目錄
  改變，`find_tw_night_markets_list` 的存檔路徑也有同樣問題。屬**候選 8**。
- **抓取層的重試、節流與狀態碼處理**：本 ADR 只讓非 200 從「靜默」變成「失敗」，
  沒有加上重試或退避。`e_*.py` 其他函式的 `return None`、
  Google `OVER_QUERY_LIMIT`（HTTP 200 但語意是失敗）等屬**候選 6**。
- **`d04` 的 mart SQL 內容本身**：本輪只改執行方式，不動 `src/task/mart_table_sql/*.sql`。

## 明確不做：把整套連線樣板抽成共用介面

`upsert_to_table`、`exec_mart_sql_files`、`exec_sql_multistatement` 三支函式
都走「取得連線 → 建 cursor → 執行 → 事務控制 → 釋放」的流程。
本輪只抽出 `close_quietly()`，**不再往上抽第二層**，
且這不是「留待日後」，而是判定不該做。

理由：這三處**只有骨架相同，內容全部不同**。

| 函式 | `except` 分層 | 日誌內容 |
| --- | --- | --- |
| `upsert_to_table` | `pymysql.MySQLError` / `Exception` 兩層，各自 rollback | 帶表名；rollback 成功另記一行 |
| `exec_mart_sql_files` | 單層 `Exception` | 帶迴圈中失敗的**檔名**（需要區域變數） |
| `exec_sql_multistatement` | 單層 `Exception` | 固定字串 |

要共用就得把「例外分類策略」與「訊息組裝」一起變成參數傳進去 ——
那是把差異搬進參數表，產生一個每個呼叫端都要先讀懂參數才會用的假共用點，
比三處各寫一次更難讀，也更難改。

`close_quietly()` 能抽出來，正是因為它**沒有這種差異**：
不論呼叫端是誰，關閉失敗都只記一行 warning。**共用點該落在沒有分歧的地方。**

未來的架構檢視若再次掃出「三處重複的連線樣板」，請以本節為準，不要重新提案。
