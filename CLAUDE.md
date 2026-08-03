# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 專案概述

臺灣交通事故 ETL + Streamlit 視覺化專案。後端（MySQL / Redis / Airflow）跑在 GCP VM 的 Docker Compose 上，前端 Streamlit 以獨立 image 部署到 Cloud Run，透過 VPC Connector 連回 VM 內網存取 MySQL 與 Redis。

README.md 描述的是「分支即里程碑」的開發流程（`feature/etl-app` → `feature/docker-integration` → `develop/CI` → `UAT` → `main`）。目前 `main` 是集大成的生產分支。

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
poetry run python -m src.task.l_fact_accident_main

# 啟動前端
poetry run streamlit run src/app.py

# 測試（pytest 無設定檔，直接指定路徑）
poetry run pytest test/unit_test/
poetry run pytest test/unit_test/test_util_paths.py::test_專案根含有_pyproject_toml

# 後端整套服務（MySQL + Redis + Airflow scheduler/triggerer/api-server）
docker compose up -d --build     # 需要根目錄有 .env
docker compose logs -f airflow-scheduler
```

Airflow UI：`http://<host>:8081`（容器內 8080 對外 8081）。

## 架構要點

### ETL 命名慣例：`e_` / `t_` / `l_`

`src/task/` 下每個檔案是一個 ETL 階段的純函式，檔名前綴即階段：

- `e_*.py` — 爬取 data.gov.tw / Google Maps API，回傳**檔案路徑 list**
- `t_*.py` — 讀 CSV → 清洗 → 回傳 **DataFrame**
- `l_*.py` — 接 DataFrame → 手組 `INSERT ... ON DUPLICATE KEY UPDATE` → `executemany` upsert 進 MySQL，回傳 `None`

三個前綴之外還有**無前綴的工具型 task**（`create_*_tables.py`、`exec_mart_sql.py`），它們不屬於 e/t/l 任一階段，用描述性檔名。

DAG 只負責串接：`dags/dNN_*.py` 把上述函式包進 `@task`，用 `pathlist` / DataFrame 在 task 間傳遞。所以**改 ETL 邏輯改 `src/task/`，改排程與相依改 `dags/`**。

資料模型是星狀綱要：`fact_accident_main` / `fact_accident_env` / `fact_accident_human` / `fact_night_markets` 搭配 `dim_accident_day` / `dim_accident_type` / `dim_road_design` / `dim_lane_design`。欄位中→英的對照集中在 `src/util/table_column_map.py`。

**六支讀事故 CSV 的 `t_*.py` 一律走 `read_traffic_accident_file(path, column_map)`** —— `docs/adr/0010-*.md`。不要在 `t_*.py` 裡自己寫 `pd.read_csv`：`skipfooter=2`（末兩行是統計備註）與 `engine="python"` 是對 data.gov.tw 檔案格式的斷言，只能有一處。

那支函式**按名稱**對應欄位（`rename` + `reindex`），缺席的欄位成為 `NaN` 並留在正確位置。改回按位置賦名會重現本輪修掉的缺陷：`t_fact_accident_human` 曾因舊表頭少一欄（`共享經濟或外送平台的名稱` 是 114 年度才新增）而讓 21 欄中的 15 欄整體錯位，59/72 個檔案受影響，390 萬列錯誤資料寫進 MySQL。**CSV 缺欄是資料來源的正常演進，不是故障，一律 raise 會讓既有檔案全部處理不了** —— 缺欄記 `warning` 並繼續。

空 `pathlist` 則相反，六支一律 `raise ValueError` —— 那代表上游沒抓到任何檔案，是故障（ADR-0003）。

`t_fact_*` 三支在中段呼叫 `get_table_from_sqlserver()` 取 FK 是**刻意保留**的，取 surrogate key 本來就是星狀綱要 transform 的職責；且 `t_fact_accident_env` / `t_fact_accident_human` 查的 `fact_accident_main` 有 213 萬列，不可能改成參數傳入。評估過程記於 ADR-0010 執行摘要第五章，不必再提議改它。

### 前端資料鏈

`src/app.py`（首頁）→ `src/pages/v_*.py`（各分頁）→ `src/task/core/`：

- `c_data_service.py` — 業務運算層（夜市周邊事故、haversine、熱區抽樣），也是 `d06_precompute_to_redis` DAG 呼叫的對象
- `c_db.py` — 純 SQL 查詢層
- `c_ui.py` — 共用 UI 元件（側邊欄等）

重運算走「DAG 預先算好 → pickle 進 Redis（TTL 10 天）→ 前端 `get_cache` 讀」的模式，前端不做重運算。`src/task/mart_table_sql/*.sql` 是 mart 層純 SQL，由 `d04_analysis_pedestrian_accidents` 以 multistatement 連線執行。

**查詢的粒度要與使用它的粒度一致** —— `docs/adr/0009-*.md`。`cal_accidents_nearby_nightmarket()` 服務的單位是一個批次，也就是 30 個夜市，所以整批只發一句 SQL：`_build_batch_bbox_query()` 把 30 個夜市各自的 3 公里方框組成 OR 聯集，逐夜市迴圈裡不得再出現查詢。

病因是查詢原本寫在那個迴圈裡面，一批就要跑 30 趟，而相鄰夜市的方框大量重疊，同一批列會被傳回好幾次；士林與寧夏兩框的重疊面積就約 36%。本機容器實測，一個批次從 1.554 秒降到 0.316 秒、傳回列數少掉 68%，其中掃描只省 39%、傳輸省 61%，收益主要來自不再重複傳輸重疊區的列。批次愈大愈划算，因為聯集省下的是 N−1 次「開一句查詢」的固定開銷。

粒度修正後，原本為了遷就逐圈查詢而長出來的東西一併失去存在理由。`time.sleep(0.05)` 的節流對象已經不存在，而 A/B 實測顯示它一個人就占了舊版總時間的 53%；`failed_markets` 想保護的那個會失敗的 I/O 也不在迴圈裡了，查詢失敗現在就是整批失敗。

有三件事不可退讓：座標一律走 bind parameter，不 f-string 內插；`traffic:nearby_v12:{lat}_{lon}_3.0_all_sample` 是 `aggregate_national_master()` 唯一會讀的契約 key，必須無條件寫入；切給每個夜市的 DataFrame 要 `reset_index(drop=True)`，否則快取內容不再與逐夜市查詢的舊行為逐列相同。


### logger 與例外處理

`src/util/logger_crtx.py` 只有一個函式 `get_logger(name)`，純標準函式庫、不偵測環境。新增模組時：

```python
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)
```

三條規則，各有 ADR 背書：

1. **不要把例外轉換成 `AirflowException`，一律原樣 `raise`** —— `docs/adr/0001-*.md`。Airflow 3 中 `AirflowException` 與任何其他例外的失敗／重試語意完全相同，轉換只會遮蔽原始錯誤型別。真的需要「失敗但不重試」時才明確使用 `AirflowFailException`。
2. **不要把故障吞成「正常但空」的回傳值** —— `docs/adr/0003-*.md`。回空 `DataFrame`／空 list／`None` 會讓「查無資料」與「服務故障」無法區分，上游監控因此失效。降級是呼叫端（前端）的政策，不是資料服務層的職責。
3. **`exc_info=True` 只用在例外停止傳播之處** —— `docs/adr/0003-*.md` 子決策 5。判準一句話：**有 `raise` 就不帶 `exc_info`，沒有 `raise` 才帶**。內層重複輸出 traceback 會淹沒真正的邊界。Airflow task 不需自行記錄（例外傳出時 Airflow 自動輸出完整 traceback）；Streamlit 前端則必須記錄，因為 `st.error()` 只給使用者看。

ADR-0001 已套用到 `src/util/` 全部工具與 `src/task/` 全部 21 個模組 —— **這些模組都能在無 Airflow 的環境（地端、pytest、Cloud Run）被匯入**，由 `test/unit_test/test_util_logger_crtx.py` 把關。`dags/` 底下仍照常 import airflow，那是它該做的事。

三條規則在 `src/` 與 `dags/` 內**皆已無例外**（`AirflowException` 零出現；`finally` 內零 `return`；有 `raise` 卻帶 `exc_info` 零處，由 AST 稽核把關）。

補充兩條後續 ADR 追加的規則：

4. **`finally` 只負責釋放資源** —— `docs/adr/0005-*.md`。裡面放 `return` 會丟棄正在傳播的例外；清理動作要有守衛（用 `mysql_utils.close_quietly()`），否則 `close()` 自身的例外會取代原例外。
5. **會被重試的失敗記 `warning`，不會被重試的記 `error`** —— `docs/adr/0006-*.md`。抓取層在 `@retry_on_transient` 之下，用 ERROR 記錄一次可自癒的 503 會讓監控放大成三筆告警。

### 共用工具（`src/util/`）

`src/util/` 的重構已完成，ADR-0001 與 0003～0008、0010 全部套用完畢，六支模組各有明確職責、有測試把關（另有 `table_column_map.py` 是純資料）：

| 模組 | 職責 |
| --- | --- |
| `mysql_utils.py` | MySQL 的**唯一存取面**：Engine 快取、綱要檢查、查詢、upsert、`close_quietly()` |
| `redis_utils.py` | Redis 連線池單例與 Pickle 快取讀寫 |
| `crawling_utils.py` | **通用**抓取能力：重試、故障分類、`fetch_soup()`、兩支下載函式 |
| `paths.py` | 所有路徑的單一基準 |
| `read_traffic_accident_file.py` | 事故 CSV 的唯一讀取契約：`skipfooter` / 挑欄 / 改名 / 去空白 |
| `logger_crtx.py` | `get_logger(name)` |

**MySQL** —— 取連線一律用 `get_engine_to_mysql(database)`，不要自己 `create_engine()`，也不要 `dispose()` 它（`docs/adr/0004-*.md`）。Engine 以 `database` 為鍵快取在模組層 `_ENGINES`，生命週期與行程等長；呼叫端關掉它等於丟棄整個連線池。Redis 側對應的是 `redis_utils._REDIS_POOL`（外部走 `create_redis_client()`）。查詢走 `get_table_from_sqlserver()`（名字說謊，查的是 MySQL），寫入走 `upsert_to_table()`。`d04` 的 multistatement 與 `upsert_to_table` 內部另走 pymysql 裸連線，那是刻意的 —— 裸連線有明確的 `close()`，不需要池。

**抓取** —— `crawling_utils` 只放「換一個資料來源仍然成立」的東西（`docs/adr/0006-*.md`）。站台專屬的解析規則、檔名篩選屬於 `src/task/e_*.py`。暫時性故障（5xx、429、連線錯誤）由 `@retry_on_transient` 自動重試三次；`verify` 預設 `True`，全專案只有 `e_crawling_traffic_accident.VERIFY_SSL = False` 一處停用（data.gov.tw 的憑證鏈有問題）。

**路徑** —— 一律從 `src.util.paths` 取，**不要用 `Path().resolve()`**（那是 CWD 不是專案根，`docs/adr/0007-*.md`）。資料落點 `RAW_DATA_DIR` / `PROCESSED_DATA_DIR` 指向 `<專案根>/data/{raw,processed}`（compose 有掛載，`.gitignore` 已排除）；程式碼資產 `MART_SQL_DIR` 由 `__file__` 推導。`test_util_paths.py` 有 AST 護欄，寫回 `Path().resolve()` 會立刻紅燈。

**`src/util/` 已無孤兒模組。** `convert_time_zone.py` 與 `validate_csv_encoding.py` 於 ADR-0010 那輪刪除（零呼叫端；後者還會在驗證失敗時刪掉使用者的檔案），`timezonefinder` 依賴一併移除。

## 環境變數

`.env`（gitignore）供 docker compose 使用；`.env.example` 是範本。實際部署時由 GitHub Secrets 注入：backend workflow 在 VM 上 `echo` 生成 `.env`，Cloud Run workflow 用 `--set-env-vars` 傳入。

共 15 個變數，分成互不重疊的兩群（已與範本檔核對過，無缺漏也無多餘）：

**Python 讀取的 9 個**（`os.getenv`，散在 `mysql_utils` / `redis_utils` / `e_crawling_nightmarket` 的模組層）

`MYSQL_HOST`、`MYSQL_PORT`、`MYSQL_USER`、`MYSQL_PASSWORD`、`MYSQL_DATABASE`、`REDIS_HOST`、`REDIS_PORT`、`REDIS_PASSWORD`、`GOOGLE_MAP_API_KEY`

其中 **`MYSQL_USER`、`MYSQL_PASSWORD`、`REDIS_PASSWORD`、`GOOGLE_MAP_API_KEY` 是必填**，缺少時會在建立連線／呼叫 API 的那一刻拋出「未設定 XXX，請檢查環境變數設置」（`docs/adr/0008-*.md`）。`*_HOST` / `*_PORT` 有預設值（`localhost` / `3306` / `6379`），缺了仍可運作。

**只有 docker compose 用的 6 個**（Python 從不讀取，但**不可刪除**）

`MYSQL_AIRFLOW_DATABASE`（Airflow metadata DB 與業務 DB 分開）、`MYSQL_ROOT_PASSWORD`、`AIRFLOW_SECRET_KEY`、`AIRFLOW_ADMIN_USER`、`AIRFLOW_ADMIN_PASSWORD`、`AIRFLOW_ADMIN_EMAIL`

這群是 Airflow 啟動、以及在 MySQL 中建立它自己的 metadata database 的必要條件。「Python 不讀取」與「可以刪除」是兩件事。

Cloud Run 上 `MYSQL_HOST` / `REDIS_HOST` 是 VM 的**內網 IP**（寫死在 workflow 中），改 VM 會需要同步改 `.github/workflows/deploy-cloud-run.yml`。

## CI/CD

推 `main` 會同時觸發兩條 workflow（`**.md` 與 `docs/**` 變動會被忽略）：

- `deploy-backend-vm.yml` — 經 IAP tunnel SSH 進 VM → 生成 `.env` → `git pull` → `docker compose up -d --build`
- `deploy-cloud-run.yml` — build `docker/Dockerfile.streamlit` → push Artifact Registry → `gcloud run deploy`

沒有 CI 測試 gate；測試需在本機自行跑過。

## Agent 工具設定

Skill 檔案的實體放在 `.agents/skills/`（**納入版控**），`.claude/skills/` 只是 symlink 且 `.claude` 已被 gitignore。所以新增 skill 後要確認 `.agents/` 有進 commit，別只看 `.claude/`。

- 以 `npx skills add <repo> -s <name> -y` 安裝；多個 skill 要重複 `-s`，逗號分隔會被判定為找不到而退回列出清單
- `skills-lock.json` 記錄來源，可用 `npx skills experimental_install` 還原
- 已裝的重構分析工具鏈：`improve-codebase-architecture`（入口，`disable-model-invocation: true`，只能由使用者輸入 `/` 觸發）→ 依賴 `codebase-design`、`grilling`、`domain-modeling`；決策定案後接 `request-refactor-plan`
- 這些 skill 預期讀 `CONTEXT.md`（領域術語表）與 `docs/adr/`（架構決策記錄）。`docs/adr/` 已建立且是重構決策的真實來源；`CONTEXT.md` 仍無，因為至今的候選都是技術債而非領域建模問題
- **每個候選的產出是兩份文件**：`docs/adr/000N-<決策>.md`（決策文，動工前寫）與 `docs/adr/000N-執行摘要-<主題>.md`（執行報告，驗收後寫）。摘要固定七章：病因定義／決策摘要／實際改動／驗證方式／同病因但尚未修復的位置／現況盤點／建議的下一步。**下一輪的候選來自上一輪摘要的第五、七章**，這是流程能接續的關鍵
- `openspec/` 是另一套獨立的規格工作流（`.claude/commands/opsx*`），與上述 skill 無關

## 已知狀態

- `test/unit_test/` 現有 173 個測試（`poetry run pytest test/unit_test/`）。命名慣例：測 `src/task/*.py` 用 `test_task_*.py`、測 `src/util/*.py` 用 `test_util_*.py`，測試函式名用中文並在 docstring 寫出「釘住的是哪個決策」。沒有 CI 測試 gate，推 `main` 前需自行跑過。
- `src/app.py` 的按鈕指向多個尚未建立的頁面（`v_act1_city_accident.py`、`v_policy_impact.py`、`v_act3_avoid.py`、`v_act6_chat.py` 等），`src/pages/` 目前只有 `v_act1_all_accident.py`，點擊會報錯。
- ETL 產出的檔案落在 `data/raw`（爬回的原始檔）與 `data/processed`（解壓後的 CSV），兩者都在 `.gitignore` 內，且 compose 有掛載 `./data:/opt/airflow/data`。**舊路徑是 `test/raw_data`，已於 ADR-0007 廢除**。
- `src/task/` 與 `src/task/core/` 有 `temp_try_*.py` 暫存檔，非正式流程的一部分。
- `pyproject.toml` 的 `[tool.poetry] packages = [{include = "src"}]` 是讓 `src.xxx` 絕對匯入能運作的關鍵；容器內則改由 compose 設定的 `PYTHONPATH=/opt/airflow/`（Airflow）與 `PYTHONPATH=/app`（Cloud Run）達成同樣效果。
- `pyproject.toml` 的依賴清單與 `requirements.txt` 不會自動同步，兩者皆納入版控，改依賴時要一起更新。
