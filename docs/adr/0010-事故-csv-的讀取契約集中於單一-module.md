# ADR-0010：事故 CSV 的讀取契約集中於單一 module

- 日期：2026-08-03
- 狀態：已採納
- 範圍：`src/task/t_dim_accident_type.py`、`t_dim_lane_design.py`、`t_dim_road_design.py`、
  `t_fact_accident_main.py`、`t_fact_accident_env.py`、`t_fact_accident_human.py`
- 相關：[ADR-0003 快取層不再吞噬例外](./0003-快取層不再吞噬例外.md)（不把故障吞成「正常但空」）、
  [ADR-0006 抓取層區分暫時性與永久性故障](./0006-抓取層區分暫時性與永久性故障.md)（什麼該進 `src/util/`）
- 來源：[ADR-0009 執行摘要](./0009-執行摘要-預計算查詢粒度.md)第八章第 3 項
  「`src/task/t_*.py` 的 CSV 讀取骨架 —— 仍需先判斷它是否構成一個候選
  （目前只有『重複』，缺少明確的失敗語意病因）」

## 名詞

本文只用這五個詞，其餘一律用白話。

| 詞 | 意思 | 在程式碼的哪裡 |
| --- | --- | --- |
| **讀取骨架** | 讀一個 CSV → 挑欄 → 改名 → 去字串空白，這四步 | 六支 `t_*.py` 的 `for` 迴圈開頭，一字不差複製六份 |
| **欄位對照表** | 中文原始欄名 → 英文欄名的 dict | `src/util/table_column_map.py` 的六個 `*_col_map` / `*_col_origin_map` |
| **舊表頭 / 新表頭** | data.gov.tw 事故 CSV 的兩種欄位組成 | 舊 51 欄（111～113 年度）、新 52 欄（114 年度起） |
| **錯位** | 欄位的**值**與**欄名**對不上，整批往前挪一格 | `t_fact_accident_human.py:38` 的 `df.columns = ...` |
| **失敗語意** | 出錯時模組回什麼、拋什麼 | 本文的核心 —— 目前六支有三種 |

## 背景

### 讀取骨架長什麼樣

六支 `t_*.py` 的開頭完全相同：

```python
for file_path in csvfile_paths:
    df = pd.read_csv(file_path, encoding="utf-8", skipfooter=2, engine="python")

    required_columns = [k for k in <某個 col_map>.keys()]
    df = df.loc[:, required_columns]                    # ← 挑欄

    renamed_required_columns = [<某個 col_map>[k] for k in required_columns]
    df.columns = renamed_required_columns               # ← 改名（按位置）

    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    ...                                                 # ← 以下各支自己的清洗
```

`skipfooter=2` 是對 data.gov.tw 檔案格式的斷言（末兩行是統計備註），
`engine="python"` 是 `skipfooter` 的強制條件。這兩個常數複製了六份，沒有一處說明它們為何存在。

ADR-0009 當時把它列為「只有重複、缺少失敗語意病因」而未採納。這個保留意見是錯的。

### 問題 1：缺欄時，六支有三種失敗語意

`t_fact_accident_human` 多做一段補償：

```python
matched_columns = [m for m in required_columns if m in df.columns]
unmatched_columns = [u for u in required_columns if u not in df.columns]
df = df.loc[:, matched_columns]
for new_col in unmatched_columns:
    df[new_col] = None                                  # ← 補在最「後面」
df.columns = renamed_required_columns                   # ← 但名字按「原順序」賦值
```

其餘五支沒有這段，缺欄直接 `KeyError`。

三種語意，同一批 `pathlist` 在 `d02` / `d03` 同時餵給六支：

| 情境 | `t_fact_accident_human` | 其餘五支 |
| --- | --- | --- |
| 缺一欄 | 靜默通過，資料錯位 | `KeyError` |
| 空 `pathlist` | `ValueError`（`pd.concat` 拋的） | `road_design` 回空 DataFrame，其餘 `ValueError` |

### 問題 2：那段補償正在產出錯誤資料

`df` 的欄位順序是 `matched + unmatched`（補的欄在最後），
但 `renamed_required_columns` 是**對照表的原順序**。
`df.columns = ...` 是**按位置**賦名，兩個順序一旦不同，缺席欄之後的每一欄都往前挪一格。

`共享經濟或外送平台的名稱` 在 `fact_accident_human_col_origin_map` 排第 16 位（共 21 個），
後面還有 5 個 key。用真實的 `data/processed/111年度A1交通事故資料.csv` 實跑：

```
英文欄名                                ← 實際裝著的原始中文欄
serving_sharing_economy_or_delivery    ← 車輛撞擊部位大類別名稱-最初   （值："機車"）
impact_point_major_initial             ← 車輛撞擊部位子類別名稱-最初   （值："前車頭"）
impact_point_minor_initial             ← 車輛撞擊部位大類別名稱-其他
impact_point_major_other               ← 車輛撞擊部位子類別名稱-其他
impact_point_minor_other               ← 肇事逃逸類別名稱-是否肇逃
hit_and_run                            ← （補上的 None）
```

`hit_and_run` 拿到的是 `None`，經
`df["hit_and_run"].apply(lambda r: 1 if r == "是" else 0)` 之後**恆為 0**。

### 問題 2 的規模：不是假設，已經在資料庫裡

`data/processed` 現有 72 個 CSV，兩種表頭：

| 表頭 | 欄數 | 檔數 | 有 `共享經濟或外送平台的名稱` |
| --- | --- | --- | --- |
| 舊 | 51 | **59** | 否 |
| 新 | 52 | 13 | 是 |

也就是 59/72 個檔案會走進錯位分支。本機 `mysql_server` 容器內的
`traffic_accidents.fact_accident_human`（共 4,814,400 列）：

| 欄位 | 實際裝的值 | 列數 |
| --- | --- | --- |
| `impact_point_minor_other` | 「否」3,856,167 ＋「是」47,901 —— 這是**肇逃**的值 | 3,904,068（81%） |
| `serving_sharing_economy_or_delivery` | 「機車」1,049,284、「汽車」1,044,006、「機車與自行車」947,621 —— 這是**車種** | 約 304 萬 |
| `impact_point_major_initial` | 「機車與自行車」478,838（車種，錯位來的）混著「前車頭」1,162,406（正確） | 混雜 |
| `hit_and_run` | 0 佔 4,803,478、1 佔 10,922 | 錯位列恆為 0 |

`serving_sharing_economy_or_delivery` 只有「其他(非共享經濟與外送平台)」775,033 列是對的，
那來自 13 個新表頭檔案。

`impact_point_minor_other` 還參與 `row_hash` 的計算
（`t_fact_accident_human.py:107`），而 `row_hash` 在 MySQL 是 `UNIQUE`。

### 問題 3：缺欄不是故障，是資料來源的演進

這一點決定了缺欄政策不能是「一律 raise」。
`共享經濟或外送平台的名稱` 是 114 年度才新增的欄位，
舊表頭沒有它是**正常的**，不是抓取失敗。一律 raise 會讓 59/72 個既有檔案全部處理不了。

所以要修的不是「該不該容錯」，而是「容錯的實作用錯了對應方式」——
按位置對應會錯位，按名稱對應不會。

### 問題 4：空 `pathlist` 的守衛只長在一支上

`t_dim_road_design.py:46` 有 `if all_df:` 守衛，空輸入回 `pd.DataFrame()`；
其餘五支讓 `pd.concat([])` 拋 `ValueError: No objects to concatenate`。

上游 `e_crawling_hist_traffic_accident()` 在找不到任何下載連結時會回空 list 而不 raise，
所以空 `pathlist` 正是「上游靜默沒抓到檔案」的訊號。
回空 DataFrame 會讓它變成「upsert 0 列、DAG 綠燈」—— ADR-0003 禁止的那種吞噬。

### 問題 5：這六支一個測試都沒有

`test/unit_test/` 現有 153 個測試，沒有一個測 `t_*.py`。
上面四個問題全部是這次探索才發現的，其中問題 2 已經污染生產資料超過一輪。

## 決策

新增 `src/task/read_accident_csv.py`，把讀取骨架收成一個 module，六支呼叫它。

```python
def read_accident_csv(csvfile_path: str | Path, column_map: dict[str, str]) -> pd.DataFrame:
    df = pd.read_csv(csvfile_path, encoding="utf-8", skipfooter=2, engine="python")

    missing = [c for c in column_map if c not in df.columns]
    if missing:
        logger.warning(f"{csvfile_path} 缺少欄位 {missing}，這些欄位將為 NaN")

    df = df.rename(columns=column_map)                          # ← 按「名稱」對應
    df = df.reindex(columns=list(column_map.values()))          # ← 缺席欄自動成 NaN，順序永遠正確
    return df.map(lambda x: x.strip() if isinstance(x, str) else x)
```

六支的呼叫端：

```python
if not csvfile_paths:
    raise ValueError("csvfile_paths 為空，上游未產出任何 CSV 檔")

for file_path in csvfile_paths:
    logger.info(f"正在處理csv檔案: {file_path}")
    df = read_accident_csv(file_path, <該支的 col_map>)
    ...                                                         # ← 以下各支自己的清洗，不動
```

### 七項子決策

1. **interface 吃單一檔案，不吃 `pathlist`。**
   六支的 `for` 迴圈保留。把迴圈也吸進去會讓「逐檔清洗」變成「concat 後清洗」，
   改變記憶體峰值並需要另外驗證等價 —— 那超出本文的病因。

2. **挑欄與改名合併成 `rename` + `reindex`，按名稱對應。**
   這是問題 2 的根治。`reindex` 同時完成三件事：挑出要的欄、丟掉不要的欄、
   把缺席的欄補成 `NaN` 且放在正確位置。錯位在結構上不可能再發生。

3. **缺欄容錯保留，但記 `warning`。**
   理由見問題 3。用 WARNING 而非 ERROR：這不是故障，但也不該無聲無息
   （ADR-0006 子決策：會被重試的記 warning、不會的記 error —— 這裡兩者皆非，
   取其「值得注意但不中止」的語意）。

4. **`t_fact_accident_human` 的 `matched` / `unmatched` 補償段整段刪除。**
   它的職責已由子決策 2 承接，留著就是第二套缺欄政策。

5. **空 `pathlist` 六支統一明確 `raise ValueError`。**
   `t_dim_road_design` 的 `if all_df:` 與 `return pd.DataFrame()` 一併拿掉。
   訊息要說出「上游未產出任何 CSV 檔」，而不是 `pd.concat` 那句看不出成因的
   `No objects to concatenate`。

6. **module 放 `src/task/` 而非 `src/util/`。**
   `skipfooter=2` 是 data.gov.tw 專屬的格式斷言，不符 ADR-0006 對 `src/util/` 的判準
   （「換一個資料來源仍然成立」）。檔名比照 `create_*_tables.py`、`exec_mart_sql.py`
   的無前綴描述性命名 —— 它不屬於 e/t/l 任一階段。

7. **既有的 390 萬列錯位資料本輪不修。**
   本文只負責程式碼。資料修復與其風險（見下）寫入執行摘要第五章。

### 一個預期中的行為變化

舊表頭其實**有**「肇事逃逸類別名稱-是否肇逃」，缺的只是「共享經濟或外送平台的名稱」。
修正後：

- `hit_and_run` 會拿到真實的「是 / 否」，不再恆為 0
- `serving_sharing_economy_or_delivery` 對舊表頭檔案才是 `NaN` —— 這正是它該有的樣子

### 為什麼不是「把 `共享經濟或外送平台的名稱` 從對照表拿掉」

那會讓 13 個新表頭檔案的該欄資料被丟棄，且 114 年度之後每新增一個欄位都要再做一次抉擇。
按名稱對應是一次解決全部，包括未來還會再變的表頭。

## 後果

**得到**

- 錯位在結構上不可能再發生 —— `reindex` 的語意保證欄名與值對齊
- 六支的缺欄語意與空輸入語意各只剩一種
- `skipfooter=2` / `engine="python"` / `encoding` 只有一處，改資料來源只動一個檔
- 六支從 0 個測試進入測試網

**付出**

- 多一個 module 與一次函式呼叫（每檔一次，72 次，可忽略）
- `df.loc[:, required_columns]` 換成 `rename` + `reindex`，讀者需要知道 `reindex` 對缺席欄的行為

**誠實的限制**

- 本文不修既有資料。`row_hash` 用到 `impact_point_minor_other`，修正後同一筆會算出不同的
  hash，下次 upsert 會**插入新列而非覆蓋**，表內將同時存在對、錯兩個版本。
  在資料修復完成前，`fact_accident_human` 的查詢結果不可信。
- 三支 `t_fact_*` 的測試需要 monkeypatch `get_table_from_sqlserver`，
  mock 出來的維度表形狀是人工假設 —— 這筆債屬於下一個候選（transform 內嵌的維度查閱）。
- `e_crawling_nightmarket.py:374` 另有一處 `pd.read_csv`，讀的是夜市 CSV、不同格式，不在本文範圍。

## 未納入本次範圍

| 項目 | 理由 |
| --- | --- |
| 既有 390 萬列錯位資料的修復 | 子決策 7 |
| 三支 `t_fact_*` 中段的 8 次 `get_table_from_sqlserver` | 改的是 interface 形狀而非失敗語意，且需動 `d02` / `d03`，屬下一個候選 |
| `validate_csv_encoding.py` / `convert_time_zone.py` 兩支孤兒 module | 與本文同主題但獨立，見 ADR-0009 執行摘要第八章第 4 項 |
| `t_fact_night_markets.py` | 讀的是 Google Maps JSON，沒有 CSV 讀取骨架 |
