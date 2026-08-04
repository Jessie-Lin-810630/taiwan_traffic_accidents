# CONTEXT

本專案的領域術語表。**只收「程式碼裡有對應實體、且講法容易分歧」的詞** ——
純技術名詞（Engine、連線池、DAG）不收，那些在 `CLAUDE.md` 與 ADR 裡說明。

新增術語時請一併指出它在程式碼的哪裡，否則這份文件會很快與實作脫節。

## 交通事故

| 詞 | 意思 | 在程式碼的哪裡 |
| --- | --- | --- |
| **事故主表** | 一筆事故一列的事實表，星狀綱要的中心 | `fact_accident_main` |
| **事故座標** | 事故發生地的經緯度，來自 data.gov.tw 的原始欄位 | `fact_accident_main.latitude` / `.longitude` |
| **舊表頭 / 新表頭** | data.gov.tw 事故 CSV 的兩種欄位組成（舊 51 欄、114 年度起 52 欄） | `src/util/read_traffic_accident_file.py`（ADR-0010） |

## 天氣觀測

| 詞 | 意思 | 在程式碼的哪裡 |
| --- | --- | --- |
| **氣象網格** | OpenMeteo 背後的天氣模式把地表切成的格子。**同一格內的所有座標拿到同一份觀測值**。2017 年起的資料來自 ECMWF IFS，緯度間距實測為 0.0703 度（約 7.8 公里） | 概念上的東西，程式碼裡沒有對應物件；`GRID_STEP` 是我們對它的近似 |
| **進位** | 把事故座標對到網格上，讓落在同一格的事故共用一次 API 請求。**不是四捨五入到小數點後幾位**，而是對到 `GRID_STEP` 的倍數 | `e_crawling_weather.round_to_weather_grid()`（ADR-0012） |
| **觀測點** | 進位後的座標。一個觀測點對應 GCS 上的一份 Parquet，也是 `fact_hourly_weather` 的 `latitude_round` / `longitude_round` | `e_crawling_weather.py` 的 `lat_round` / `lon_round` |
| **高程降尺度** | OpenMeteo 依座標海拔調整氣溫的機制。本專案**刻意停用它的預設行為**，改以固定高程請求，否則同一網格內的觀測值會不一致，進位就失去意義 | `FIXED_ELEVATION_M`（ADR-0012） |
| **批次** | 一次 API 請求涵蓋的觀測點集合，預設 50 個。批次是 Airflow dynamic task mapping 的單位 | `prep_batch_plan()` 的 `batch_size` |

### 為什麼「進位」與「觀測點」要分開講

事故座標有 11,000 個，觀測點只有數百個 —— 多個事故共用一個觀測點是正常的，
不是資料重複。`t_dataclr_weather_hist` 的 merge 就是靠觀測點把兩邊接起來。

## 夜市

| 詞 | 意思 | 在程式碼的哪裡 |
| --- | --- | --- |
| **批次** | 預計算周邊事故時，一次 SQL 涵蓋的夜市集合（30 個）。**與天氣的批次是不同的東西**，只是恰好同名 | `cal_accidents_nearby_nightmarket()`（ADR-0009） |
| **方框** | 夜市中心往外 3 公里的矩形範圍，用來先粗篩事故 | `_build_batch_bbox_query()` |
