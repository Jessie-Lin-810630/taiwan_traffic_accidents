# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**細節在 `docs/adr/`。** 本檔只寫「該遵守什麼」與「去哪裡看為什麼」，
不複述論證。決策文是 `000N-<決策>.md`，執行報告是 `000N-執行摘要-<主題>.md`。
領域術語表在 `CONTEXT.md`。

## 專案概述

臺灣交通事故 ETL + Streamlit 視覺化專案。後端（MySQL / Redis / Airflow）跑在 GCP VM 的 Docker Compose 上，前端 Streamlit 以獨立 image 部署到 Cloud Run，透過 VPC Connector 連回 VM 內網存取 MySQL 與 Redis。

README.md 描述「分支即里程碑」的開發流程（`feature/etl-app` → `feature/docker-integration` → `develop/CI` → `UAT` → `main`）。目前 `main` 是集大成的生產分支。

## 常用指令

專案根目錄必須是所有指令的執行位置（模組以 `src.xxx` 絕對匯入，靠 CWD 或 `PYTHONPATH` 解析）。

```bash
# 安裝依賴（Poetry 是開發端的真實來源；requirements.txt 是 poetry export 的產物，只給容器用）
poetry env use $(which python3.12)
poetry install

# 依賴變動後必須重新匯出，否則兩個 Dockerfile 裝到的版本會與本機不一致
poetry export -f requirements.txt --output requirements.txt --without-hashes

# 單獨執行某個 ETL 任務（不經 Airflow）
poetry run python -m src.task.e_crawling_traffic_accident

# 啟動前端
poetry run streamlit run src/app.py

# 測試（pytest 無設定檔，直接指定路徑）
poetry run pytest test/unit_test/

# 後端整套服務（MySQL + Redis + Airflow scheduler/triggerer/api-server）
docker compose up -d --build     # 需要根目錄有 .env
docker compose logs -f airflow-scheduler
```

Airflow UI：`http://<host>:8081`（容器內 8080 對外 8081）。

## 架構要點

### ETL 命名慣例：`e_` / `t_` / `l_`

`src/task/` 下每個檔案是一個 ETL 階段的純函式，檔名前綴即階段：

- `e_*.py` — 抓取外部資料源，回傳**檔案路徑 list**（天氣是例外，見下節）
- `t_*.py` — 讀 CSV → 清洗 → 回傳 **DataFrame**
- `l_*.py` — 接 DataFrame → `upsert_to_table()` 寫進 MySQL，回傳 `None`

另有**無前綴的工具型 task**（`create_*_tables.py`、`exec_mart_sql.py`），不屬於任一階段。

DAG 只負責串接：`dags/dNN_*.py` 把上述函式包進 `@task`。**改 ETL 邏輯改 `src/task/`，改排程與相依改 `dags/`。**

資料模型是星狀綱要：`fact_accident_main` / `fact_accident_env` / `fact_accident_human` / `fact_night_markets` / `fact_hourly_weather`，搭配 `dim_accident_day` / `dim_accident_type` / `dim_road_design` / `dim_lane_design`。欄位中→英對照集中在 `src/util/table_column_map.py`。

**六支讀事故 CSV 的 `t_*.py` 一律走 `read_traffic_accident_file(path, column_map)`** —— ADR-0010。不要自己寫 `pd.read_csv`；那支函式**按名稱**對應欄位，改回按位置賦名會重現「15 欄整體錯位、390 萬列錯誤資料」的缺陷。

- **CSV 缺欄是資料來源的正常演進，不是故障** —— 記 `warning` 並繼續
- **空 `pathlist` 相反，一律 `raise ValueError`** —— 上游沒抓到任何檔案是故障（ADR-0003）
- `t_fact_*` 三支在中段查 FK 是**刻意保留**的（ADR-0010 執行摘要第五章），不必再提議改它

### 天氣 ETL（`e_`/`t_`/`l_fact_hourly_weather`）

OpenMeteo → GCS Parquet → MySQL。與其他 pipeline 的差別是**有外部額度限制**
（每分 600／每時 5,000／每日 10,000，加權計費，1 次額度 ≈ 25 個「觀測點 × 天」）。

**GCS 的路徑就是抓取進度表**（ADR-0013）：

```
weather_cache_final/{年}/data/{年}-{月}/{lat}_{lon}.parquet   ← 檔案在 = 該觀測點該月抓到了
weather_cache_final/{年}/tmp/{年}-{月}/batch_no_{批號}.parquet ← XCom 交接用，每次 run 覆蓋
```

四條不可退讓：

1. **檔名一律走 `blob_file_name(lat, lon)`。** 盤點與存檔兩處必須同一支 ——
   分岔過一次，`isin()` 恆為 False，續跑機制形同不存在，pipeline 零產出。
2. **完成判定是 `is_month_complete()`：`M 的最後一天 <= today - 3`**，不是「月已過去」。
   API 有 3 天延遲，用後者會讓每個月的最後 3 天永久缺失且不報錯。
3. **經緯度兩側都走 `round_to_weather_grid()`**（ADR-0012）。只要一側用別的算法，
   `t_fact_hourly_weather` 的 merge 會一列都對不上，而且不會報錯。
4. **批次 = 一批觀測點 × 一個月**，`prep_batch_plan()` 回傳 `list[dict]`，
   DAG 走 `expand_kwargs()`。回傳空 list 是正常結果（已全部抓完），不是故障。

`d07` 每月 3、18 號跑（3 號抓剛完成的上個月、18 號抓當月 1～15 日）；
`d08` 是一次性的四年回補，補完後**手動 pause**。兩者的 `load` task 都是
`trigger_rule="all_done"` —— 抓取被額度打斷是正常狀態，已落地的資料要先進 MySQL。

### 前端資料鏈

`src/app.py`（首頁）→ `src/pages/v_*.py`（各分頁）→ `src/task/core/`：

- `c_data_service.py` — 業務運算層（夜市周邊事故、haversine、熱區抽樣），也是 `d06_precompute_to_redis` 呼叫的對象
- `c_db.py` — 純 SQL 查詢層
- `c_ui.py` — 共用 UI 元件

重運算走「DAG 預先算好 → pickle 進 Redis（TTL 10 天）→ 前端 `get_cache` 讀」，前端不做重運算。`src/task/mart_table_sql/*.sql` 由 `d04` 以 multistatement 執行。

**查詢的粒度要與使用它的粒度一致** —— ADR-0009。`cal_accidents_nearby_nightmarket()` 服務的單位是一個批次（30 個夜市），所以整批只發一句 SQL（`_build_batch_bbox_query()` 組 OR 聯集），**逐夜市迴圈裡不得再出現查詢**。實測一批從 1.554 秒降到 0.316 秒。

三件不可退讓：座標走 bind parameter，不 f-string 內插；`traffic:nearby_v12:{lat}_{lon}_3.0_all_sample` 是 `aggregate_national_master()` 唯一會讀的契約 key，必須無條件寫入；切給每個夜市的 DataFrame 要 `reset_index(drop=True)`。

### logger 與例外處理

`src/util/logger_crtx.py` 只有 `get_logger(name)`，純標準函式庫、不偵測環境。

```python
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)
```

五條規則，各有 ADR 背書，**在 `src/` 與 `dags/` 內皆已無例外**（由 AST 稽核把關）：

1. **不要把例外轉換成 `AirflowException`，一律原樣 `raise`**（ADR-0001）。轉換只會遮蔽原始錯誤型別。需要「失敗但不重試」才用 `AirflowFailException`。
2. **不要把故障吞成「正常但空」的回傳值**（ADR-0003）。回空 `DataFrame`／空 list／`None` 會讓「查無資料」與「服務故障」無法區分。降級是呼叫端的政策，不是資料服務層的職責。
3. **`exc_info=True` 只用在例外停止傳播之處**（ADR-0003 子決策 5）。判準：**有 `raise` 就不帶，沒有 `raise` 才帶**。Airflow task 不需自行記錄；Streamlit 前端必須記錄。
4. **`finally` 只負責釋放資源**（ADR-0005）。裡面放 `return` 會丟棄正在傳播的例外；清理要有守衛（`mysql_utils.close_quietly()`）。
5. **會被重試的失敗記 `warning`，不會被重試的記 `error`**（ADR-0006）。

ADR-0001 已套用到 `src/util/` 與 `src/task/` 全部模組 —— **它們都能在無 Airflow 的環境（地端、pytest、Cloud Run）被匯入**，由 `test_util_logger_crtx.py` 把關。`dags/` 底下照常 import airflow。

### 共用工具（`src/util/`）

七支模組各有明確職責、有測試把關（另有 `table_column_map.py` 是純資料）：

| 模組 | 職責 |
| --- | --- |
| `mysql_utils.py` | MySQL 的**唯一存取面**：Engine 快取、綱要檢查、查詢、upsert、`close_quietly()` |
| `redis_utils.py` | Redis 連線池單例與 Pickle 快取讀寫 |
| `gcs_utils.py` | GCS 的唯一存取面：`read_parquet` / `write_parquet` / `list_parquet` |
| `crawling_utils.py` | **通用**抓取能力：重試、故障分類、`fetch_soup()`、兩支下載函式 |
| `paths.py` | 所有路徑的單一基準 |
| `read_traffic_accident_file.py` | 事故 CSV 的唯一讀取契約 |
| `logger_crtx.py` | `get_logger(name)` |

**MySQL** —— 取連線一律用 `get_engine_to_mysql(database)`，不要自己 `create_engine()`，也不要 `dispose()` 它（ADR-0004）。Engine 以 `database` 為鍵快取在 `_ENGINES`，關掉它等於丟棄整個連線池。查詢走 `get_table_from_sqlserver()`（名字說謊，查的是 MySQL），寫入走 `upsert_to_table()`。`d04` 與 `upsert_to_table` 內部走 pymysql 裸連線，那是刻意的。

**抓取** —— `crawling_utils` 只放「換一個資料來源仍然成立」的東西（ADR-0006）。站台專屬的解析規則屬於 `src/task/e_*.py`。`verify` 預設 `True`，全專案只有 `e_crawling_traffic_accident.VERIFY_SSL = False` 一處停用。

**路徑** —— 一律從 `src.util.paths` 取，**不要用 `Path().resolve()`**（那是 CWD 不是專案根，ADR-0007）。`test_util_paths.py` 有 AST 護欄。

## 環境變數

`.env`（gitignore）供 docker compose 使用；`.env.example` 是範本。部署時由 GitHub Secrets 注入：backend workflow 在 VM 上 `echo` 生成 `.env`，Cloud Run workflow 用 `--set-env-vars`。

共 15 個，分成互不重疊的兩群：

**Python 讀取的 9 個**（`os.getenv`，散在 `mysql_utils` / `redis_utils` / `e_crawling_nightmarket` 的模組層）

`MYSQL_HOST`、`MYSQL_PORT`、`MYSQL_USER`、`MYSQL_PASSWORD`、`MYSQL_DATABASE`、`REDIS_HOST`、`REDIS_PORT`、`REDIS_PASSWORD`、`GOOGLE_MAP_API_KEY`

其中 **`MYSQL_USER`、`MYSQL_PASSWORD`、`REDIS_PASSWORD`、`GOOGLE_MAP_API_KEY` 必填**，缺少時在使用的那一刻拋出（ADR-0008）。`*_HOST` / `*_PORT` 有預設值。

**只有 docker compose 用的 6 個**（Python 從不讀取，但**不可刪除**）

`MYSQL_AIRFLOW_DATABASE`、`MYSQL_ROOT_PASSWORD`、`AIRFLOW_SECRET_KEY`、`AIRFLOW_ADMIN_USER`、`AIRFLOW_ADMIN_PASSWORD`、`AIRFLOW_ADMIN_EMAIL`

這群是 Airflow 啟動與建立自己的 metadata database 的必要條件。「Python 不讀取」與「可以刪除」是兩件事。

**GCS 不新增任何環境變數** —— `gcs_utils` 走 Application Default Credentials：VM 上取用附加的 service account，地端取用 `gcloud auth application-default login` 留下的憑證。

Cloud Run 上 `MYSQL_HOST` / `REDIS_HOST` 是 VM 的**內網 IP**（寫死在 workflow 中），改 VM 要同步改 `.github/workflows/deploy-cloud-run.yml`。

## CI/CD

推 `main` 會同時觸發兩條 workflow（`**.md` 與 `docs/**` 變動會被忽略）：

- `deploy-backend-vm.yml` — 經 IAP tunnel SSH 進 VM → 生成 `.env` → `git pull` → `docker compose up -d --build`
- `deploy-cloud-run.yml` — build `docker/Dockerfile.streamlit` → push Artifact Registry → `gcloud run deploy`

沒有 CI 測試 gate；測試需在本機自行跑過。

## Agent 工具設定

Skill 實體放在 `.agents/skills/`（**納入版控**），`.claude/skills/` 只是 symlink 且 `.claude` 已被 gitignore。新增 skill 後要確認 `.agents/` 有進 commit。

- 以 `npx skills add <repo> -s <name> -y` 安裝；多個 skill 要重複 `-s`
- 重構分析工具鏈：`improve-codebase-architecture`（入口，只能由使用者輸入 `/` 觸發）→ 依賴 `codebase-design`、`grilling`、`domain-modeling`
- **每個候選的產出是兩份文件**：決策文（動工前）與執行摘要（七章：病因定義／決策摘要／實際改動／驗證方式／同病因但尚未修復的位置／現況盤點／建議的下一步）。**下一輪的候選來自上一輪摘要的第五、七章**，這是流程能接續的關鍵
- **ADR 生效後不再改動**。實作期的決定與偏離寫進執行摘要，不回頭改決策文
- `openspec/` 是另一套獨立的規格工作流，與上述無關

## 已知狀態

- `test/unit_test/` 現有 **193 個測試**。命名慣例：測 `src/task/*.py` 用 `test_task_*.py`、測 `src/util/*.py` 用 `test_util_*.py`，測試函式名用中文並在 docstring 寫出「釘住的是哪個決策」。
- **天氣 ETL 已實作、待驗收**（ADR-0013 執行摘要第四章）。`t_`/`l_fact_hourly_weather` **至今未在真實資料上跑過**，`fact_hourly_weather` 是空的。VM 上要先清掉 GCS 舊檔名格式的檔案，再驗 DAG parse 與 `expand_kwargs`。
- `src/app.py` 的按鈕指向多個尚未建立的頁面，`src/pages/` 目前只有 `v_act1_all_accident.py`，點擊會報錯。
- ETL 產出落在 `data/raw` 與 `data/processed`，皆在 `.gitignore` 內，compose 有掛載。**舊路徑 `test/raw_data` 已於 ADR-0007 廢除**。
- `src/task/` 與 `src/task/core/` 有 `temp_try_*.py` 暫存檔，非正式流程的一部分。
- `pyproject.toml` 的 `[tool.poetry] packages = [{include = "src"}]` 是讓 `src.xxx` 絕對匯入能運作的關鍵；容器內改由 `PYTHONPATH=/opt/airflow/`（Airflow）與 `PYTHONPATH=/app`（Cloud Run）達成。
- `pyproject.toml` 與 `requirements.txt` 不會自動同步，改依賴時要一起更新。
