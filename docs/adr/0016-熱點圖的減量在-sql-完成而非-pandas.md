# ADR-0016：熱點圖的減量在 SQL 完成，而非 pandas

- 日期：2026-08-10
- 狀態：已採納
- 範圍：`src/task/core/c_db.py`、`src/task/core/c_data_service.py`、`src/app.py`
- 相關：[ADR-0009 預計算的查詢粒度以批次為單位](./0009-預計算的查詢粒度以批次為單位.md)
  （「查詢的粒度要與使用它的粒度一致」，本 ADR 是同一條原則用在減量上）、
  [ADR-0003 快取層不再吞噬例外](./0003-快取層不再吞噬例外.md)
  （read-through 的失敗語意不變）、
  [ADR-0015 事故事實表的載入以檔案批次為單位](./0015-事故事實表的載入以檔案批次為單位.md)
  （同一個病因的另一面：記憶體需求由資料量決定，而非由機器決定）
- 來源：Streamlit 部署到 Cloud Run 後，首頁載入即被 OOM 殺掉。
  1 GiB 時 `Memory limit of 1024 MiB exceeded with 1028 MiB used`，
  調到 2 GiB 後 `Memory limit of 2048 MiB exceeded with 2200 MiB used`

## 名詞

**減量**：把「事故主檔的每一列」壓成「地圖上的每一個熱點」的過程。
現行實作是三層：國土範圍過濾 → 按座標 `groupby` 取 `count >= 3` → 超過
`sample_size` 就隨機抽樣。

**下推（push down）**：把減量從應用程式移到 SQL，讓 MySQL 只回傳減量後的結果。

## 背景

### 病因：減量發生在下載之後

`get_accident_heatmap_data()`（`c_data_service.py:173`）是 read-through 快取：
先讀 Redis，沒命中就呼叫 `get_accident_table_with_main_day()`（`c_db.py:41`）。

而後者在兩個日期都不給時**撈全表** —— `fact_accident_main JOIN dim_accident_day`
的十個欄位，沒有 `WHERE`、沒有 `GROUP BY`。三層減量全部發生在資料回到 pandas
之後：

```
MySQL ──全表──▶ pandas ──過濾──▶ groupby ──抽樣──▶ 8000 列
        ↑
     峰值在這裡
```

要拿到 8000 個熱點，得先把整張事故主檔吃進記憶體。**記憶體需求由資料量決定，
與執行它的容器有多少記憶體無關** —— 這與 ADR-0015 在 d03 上遇到的是同一個病因，
只是換到了前端。

峰值還不只是 DataFrame 本身。`get_accident_table_with_main_day()` 在回傳前會做
一次 `df.apply(..., axis=1)` 組 `tooltip_text`（`c_db.py:127`）：逐列呼叫 Python
函式、每列產生一個三行字串，再多掛一個 object 欄位到同一張表上。這一步的成本
與列數成正比，且**熱點圖用不到 tooltip** —— 它 groupby 之後只留
`latitude`、`longitude`、`count` 三欄。

### 為什麼現在才發作

**VM 上沒有跑過前端。** `docker-compose.yml` 的服務只有 `mysql-db`、
`redis-cache`、`airflow-init`／`webserver`／`scheduler`／`triggerer`，
沒有 streamlit（2026-08-10 查證）。部署到 Cloud Run 之前，前端只在開發者本機以
`poetry run streamlit run src/app.py` 執行過。

因此：

- 這段程式碼**第一次在有硬性記憶體上限的環境執行，就是這次的 Cloud Run**
- 本機執行時是否曾經成功撈完全表、`traffic:global_heatmap_lite_v2` 是否
  曾經被寫進生產環境的 Redis，**都未查證**

能確定的只有一件事：Cloud Run 上這條路徑從未成功過。第一次全表載入就被砍，
所以快取永遠寫不進去，每次冷啟動都重演 —— 這也解釋了為什麼調高記憶體上限
只是讓它在被砍前多撈一點。

**不能說「Cloud Run 讓既有的問題浮現」**，因為沒有證據顯示它在別處是可行的。
準確的說法是：這是一段缺乏記憶體上限約束下寫成的程式碼，第一次被放進有上限的
環境。

### 這個快取沒有消費端

盤點 `traffic:global_heatmap_lite_v2` 的讀寫（2026-08-10）：

| 位置 | 行為 |
| --- | --- |
| `c_data_service.py:198` | 讀，沒命中則查 MySQL 後寫回 |
| `app.py:105` | 呼叫 `get_accident_heatmap_data()`，註解自陳「僅為預熱 Redis 快取，回傳值本頁未使用」 |
| `temp_try_c_data_service_origin.py:156` | 暫存檔，非正式流程 |

三個分頁讀的是別的鍵 —— `v_act1_all_accident.py:83` 與 `v_act1_city_accident.py:84`
讀 `market:national_master_df`，`v_act1_single_accident.py:110` 讀
`traffic:nearby_v12:{lat}_{lon}_3.0_all_sample`。**沒有任何頁面讀熱點圖的鍵。**

`app.py` 的版面也確實沒有地圖：docstring 寫「顯示全臺事故熱力圖與關鍵數字」，
但 `main()` 實際畫出來的是三個數字卡片、痛點引言、受眾價值卡片與四個分頁按鈕，
數字還是寫死在 HTML 裡的（`300+`、`150 萬+`、`5+`）。**docstring 描述的是一個
沒有被實作、或曾經存在後來移除的版面。**

`d06_precompute_to_redis` 也沒有預算這個鍵（它算的是夜市周邊事故那一組），
所以「由 DAG 預先算好、前端只讀」的架構在這條路徑上並不成立。

### 量到的數字

| 數字 | 值 | 等級 | 怎麼來的 |
| --- | --- | --- | --- |
| Cloud Run 1 GiB 時的用量 | 1028 MiB | **實測** | Cloud Run 系統日誌，2026-08-10 |
| Cloud Run 2 GiB 時的用量 | 2200 MiB | **實測** | 同上 |
| `fact_accident_main` 列數 | — | **未量測** | 需連 VM 上的 MySQL 才能查 |
| 事故 CSV 的總列數 | 4,359,391 | **實測** | ADR-0015 量的，五年 65 個檔案 `wc -l`。**這是 CSV 的列數，不等於主檔列數**（主檔經 upsert 收斂） |
| 減量後的熱點數 | — | **未量測** | 取決於不重複座標數與 `count >= 3` 的分布 |
| `tooltip_text` 的每列成本 | — | **未量測** | 三行字串，`apply(axis=1)` 逐列產生 |

**兩次 OOM 的數字（1028、2200）都只是被砍當下的取樣值，不是峰值上限。**
它們證明「2 GiB 不夠」，不能用來推算「幾 GiB 才夠」—— 事實上調高上限只會讓它
在被砍之前多撈一點，兩次的用量都緊貼著當時的上限，就是這個現象。

## 決策

### 一、首頁不再呼叫 `get_accident_heatmap_data()`

移除 `app.py:105`。理由是這行呼叫的產物沒有人使用：頁面本身不畫地圖，
其他分頁讀的是別的鍵。

這一條單獨就足以解掉眼前的 OOM，而且不改變任何使用者看得到的行為。

同時修正 `main()` 的 docstring —— 它現在宣稱「載入全臺事故與夜市資料」與
「顯示全臺事故熱力圖」，移除後只剩夜市主檔（`ds.get_all_nightmarkets()`）。
**留著錯誤的 docstring 會讓下一個人以為熱點圖還在用，把這行加回去。**

`try/except` 區塊保留：`get_all_nightmarkets()` 仍然可能拋出，前端仍然是例外
停止傳播之處，`logger.error(..., exc_info=True)` 加 `st.stop()` 的處理不變
（ADR-0003）。

### 二、減量下推到 SQL

`get_accident_heatmap_data()` 改為呼叫一支新的查詢函式，讓 MySQL 完成前兩層
減量，回傳的就是熱點：

```sql
SELECT latitude, longitude, COUNT(*) AS count
FROM fact_accident_main
WHERE latitude  BETWEEN :lat_min AND :lat_max
  AND longitude BETWEEN :lon_min AND :lon_max
GROUP BY latitude, longitude
HAVING COUNT(*) >= :min_count
```

四個邊界值與 `min_count` **一律走 bind parameter，不 f-string 內插**
（與 ADR-0009 的三條不可退讓同一條規矩）。

三件跟著決定的事：

- **不 JOIN `dim_accident_day`。** 現行查詢 JOIN 它是為了取
  `accident_date` / `accident_weekday` / `is_holiday` / `national_activity`，
  而熱點圖一個都不用。
- **不組 `tooltip_text`。** 熱點是聚合後的座標，不對應單一事故，本來就組不出
  「事故日期時間 / 死亡 / 受傷」。
- **`dropna` 由 SQL 的 `BETWEEN` 順帶完成** —— `NULL` 不滿足任何比較，
  自動被排除，不需要再在 pandas 裡 `dropna`。

### 三、抽樣留在 pandas

第三層減量（超過 `sample_size` 就抽樣）**不下推**。

`df.sample(n=sample_size, random_state=42)` 的固定亂數種子是「同一份資料每次抽出
同一批點」的保證，換成 `ORDER BY RAND() LIMIT` 會失去可重現性，而且是對聚合結果
再排序一次。決策二之後進到 pandas 的已經是熱點而不是事故，抽樣的成本與那個量成
正比，不再是問題。

**分界線是：能大幅減少傳輸量的下推，不能的留在原地。**

### 四、`get_accident_table_with_main_day()` 的日期改為必填

它現在唯一的呼叫者是 `get_accident_heatmap_data()`（2026-08-10 全庫盤點，
另有 `temp_try_c_data_service_origin.py` 但非正式流程，測試也沒有覆蓋）。
決策二之後它會沒有呼叫者。

**不刪它** —— 它是一支語意完整、有日期參數、有 `tooltip_text` 的查詢，
未來要做「某段期間的事故點位圖」時會用到。依 CLAUDE.md 的規矩，
發現的無用程式碼要指出而不是順手刪掉。

**但要改掉「不給日期就撈全表」。** 現行簽章是
`start_date: tuple[int] | None = None, end_date: tuple[int] | None = None`，
把「撈走整張事故主檔」放在最省事的呼叫方式上 —— 這正是本次 OOM 的近因。
改成兩個參數都必填、沒有預設值：

```python
def get_accident_table_with_main_day(
    start_date: tuple[int, int, int], end_date: tuple[int, int, int]
) -> pd.DataFrame:
```

`WHERE` 子句與 `params` 隨之變成無條件組出，原本「兩個都不給」的分支消失。
函式內既有的兩道檢查（必須成對、必須是三個整數）保留 —— 前者的作用從
「擋住只給一個」變成由簽章擋掉，後者仍然需要。

趁沒有呼叫者的時候改契約，成本最低。這與 ADR-0003 的精神一致：
**危險的行為不該是預設值。**

### 五、查無資料時回傳空 DataFrame，不回傳 `[]`

`get_accident_heatmap_data()` 現在在 `df.empty` 時 `return []`
（`c_data_service.py:208`），其他路徑回傳 DataFrame。

這不是 ADR-0003 說的「把故障吞成空」—— 查無資料確實是正常結果，不是故障。
問題單純是型別不一致：呼叫端拿到的東西可能是 DataFrame 也可能是 list，
`.empty`、`len()`、欄位存取的行為都不同。

改為回傳空 DataFrame，讓「有資料」與「沒資料」的型別一致，
呼叫端用同一套寫法處理。本輪已經要動這支函式，一起改。

## 後果

### 正面

- 首頁載入不再撈事故主檔，記憶體佔用回到「夜市主檔 + Streamlit 本身」的量級
- `--memory` 可以從 2 GiB 調回 1 GiB（實跑確認後再調，見〈尚未決定的事〉）
- 熱點圖函式被重新啟用時是安全的：傳輸量正比於熱點數，不是事故數
- 冷啟動不再需要「先成功撈一次全表」才能建立快取

### 負面

- **`count >= 3` 這層過濾從 pandas 移到 SQL，語意的載體從程式碼變成 SQL 字串。**
  現行的 `df[df["count"] >= 3]` 一眼看得出在做什麼，`HAVING COUNT(*) >= :min_count`
  要連著查詢一起讀。以查詢字串附註解緩解。
- **`GROUP BY latitude, longitude` 的效率取決於索引，未量測。**
  `fact_accident_main` 上若沒有 `(latitude, longitude)` 的複合索引，MySQL 會走
  全表掃描 + 暫存表。那對 MySQL 是可承受的（它不需要把結果全部具現化給客戶端），
  但查詢時間可能不短。實跑後若太慢，加索引是後續決定，不在本 ADR 範圍。
- 座標的浮點相等性由 MySQL 的欄位型別決定，而不再是 pandas 的 `float64`。
  兩者對「同一個座標」的判定應該一致（都是從同一個欄位讀出來的值），
  但**未驗證**。
- **兩支函式的契約都變了**：`get_accident_table_with_main_day()` 少了無參數的
  呼叫方式，`get_accident_heatmap_data()` 查無資料時的回傳型別從 list 變成
  DataFrame。兩者目前都沒有其他呼叫者、也沒有測試覆蓋（2026-08-10 盤點），
  所以這一輪改的代價最低；但若有未盤點到的呼叫端，會是破壞性變更。

### 中性

- Redis 的鍵名與 TTL（`traffic:global_heatmap_lite_v2`、12 小時）不變
- read-through 的失敗語意不變：讀取失敗拋出，寫入失敗記 `warning` 後照常回傳
- `sample_size=8000` 的預設值與抽樣種子不變

## 尚未決定的事

- **`--memory` 要不要調回 1 GiB。** 決策一之後首頁的實際用量未量測。
  Cloud Run 的記憶體是按配置量計費，調回去有成本上的意義，但要先看實跑數字。
- **熱點圖到底要不要有。** 本 ADR 把函式修好，但沒有回答「首頁該不該畫熱力圖」。
  如果答案是要，那還有一個問題：它應該像 `market:national_master_df` 那樣由
  `d06` 預先算好，而不是靠前端 read-through 補寫 —— 前端補寫意味著「第一個
  訪客要等完整查詢跑完」。那是 ADR-0009 那條原則的延伸，屆時另議。
