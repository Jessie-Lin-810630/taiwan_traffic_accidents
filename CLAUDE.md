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
poetry run pytest test/
poetry run pytest test/unit_test/test_task_t_fact_accident_main.py::test_xxx

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

DAG 只負責串接：`dags/dNN_*.py` 把上述函式包進 `@task`，用 `pathlist` / DataFrame 在 task 間傳遞。所以**改 ETL 邏輯改 `src/task/`，改排程與相依改 `dags/`**。

資料模型是星狀綱要：`fact_accident_main` / `fact_accident_env` / `fact_accident_human` / `fact_night_markets` 搭配 `dim_accident_day` / `dim_accident_type` / `dim_road_design` / `dim_lane_design`。欄位中→英的對照集中在 `src/util/table_column_map.py`。

### 前端資料鏈

`src/app.py`（首頁）→ `src/pages/v_*.py`（各分頁）→ `src/task/core/`：

- `c_data_service.py` — 業務運算層（夜市周邊事故、haversine、熱區抽樣），也是 `d06_precompute_to_redis` DAG 呼叫的對象
- `c_db.py` — 純 SQL 查詢層
- `c_ui.py` — 共用 UI 元件（側邊欄等）

重運算走「DAG 預先算好 → pickle 進 Redis（TTL 10 天）→ 前端 `get_cache` 讀」的模式，前端不做重運算。`src/task/mart_table_sql/*.sql` 是 mart 層純 SQL，由 `d04_analysis_pedestrian_accidents` 以 multistatement 連線執行。

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

ADR-0001 已套用到 `src/util/` 全部工具與 `src/task/` 全部 20 個 ETL 模組 —— **這些模組都能在無 Airflow 的環境（地端、pytest、Cloud Run）被匯入**，由 `test/unit_test/test_util_logger_crtx.py` 把關。`dags/` 底下仍照常 import airflow，那是它該做的事。

規則 1 全 codebase 已無例外（`AirflowException` 零出現）。規則 2、3 在 `src/` 內已無殘留，`dags/d04` 仍有 `finally: return` 吞例外的殘留。

### 連線工具（`src/util/`）

連線層的重構已完成，`src/util/` 只剩一套工具：

| 模組 | 職責 |
| --- | --- |
| `mysql_utils.py` | MySQL 的**唯一存取面**：Engine 快取、綱要檢查、查詢、upsert |
| `redis_utils.py` | Redis 連線池單例與 Pickle 快取讀寫 |
| `crawling_utils.py` | 抓取工具，**尚未接上**（見下） |
| `logger_crtx.py` | `get_logger(name)` |

**取 MySQL 連線一律用 `get_engine_to_mysql(database)`，不要自己 `create_engine()`，也不要 `dispose()` 它** —— 理由見 `docs/adr/0004-*.md`。Engine 以 `database` 為鍵快取在模組層 `_ENGINES`，生命週期與行程等長；呼叫端關掉它等於丟棄整個連線池。Redis 側對應的是 `redis_utils._REDIS_POOL`（外部走 `create_redis_client()`）。

查詢走 `mysql_utils.get_table_from_sqlserver()`（名字說謊，查的是 MySQL 不是 SQL Server），寫入走 `upsert_to_table()`。`d04` 的 multistatement 與 `upsert_to_table` 內部另走 pymysql 裸連線，那是刻意的 —— 裸連線有明確的 `close()`，不需要池。

已知阻礙：`crawling_utils.download_and_extract_zip()` 的參數順序與現有呼叫端不相容，這是它接上 `e_crawling_traffic_accident.py` 的卡點。`convert_time_zone.py` 與 `validate_csv_encoding.py` 是零呼叫端的孤兒模組，去留未定。

## 環境變數

`.env`（gitignore）供 docker compose 使用；`.env.example` 是範本。實際部署時由 GitHub Secrets 注入：backend workflow 在 VM 上 `echo` 生成 `.env`，Cloud Run workflow 用 `--set-env-vars` 傳入。

關鍵變數：`MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DATABASE/MYSQL_ROOT_PASSWORD`、`MYSQL_AIRFLOW_DATABASE`（Airflow metadata DB 與業務 DB 分開）、`REDIS_HOST/REDIS_PORT/REDIS_PASSWORD`、`GOOGLE_MAP_API_KEY`、`AIRFLOW_SECRET_KEY`、`AIRFLOW_ADMIN_USER`、`AIRFLOW_ADMIN_PASSWORD`、`AIRFLOW_ADMIN_EMAIL`。

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

- `test/unit_test/` 現有 52 個測試（`poetry run pytest test/unit_test/`）。命名慣例：測 `src/task/*.py` 用 `test_task_*.py`、測 `src/util/*.py` 用 `test_util_*.py`，測試函式名用中文並在 docstring 寫出「釘住的是哪個決策」。沒有 CI 測試 gate，推 `main` 前需自行跑過。
- `src/app.py` 的按鈕指向多個尚未建立的頁面（`v_act1_city_accident.py`、`v_policy_impact.py`、`v_act3_avoid.py`、`v_act6_chat.py` 等），`src/pages/` 目前只有 `v_act1_all_accident.py`，點擊會報錯。
- `src/task/` 與 `src/task/core/` 有 `temp_try_*.py` 暫存檔，非正式流程的一部分。
- `pyproject.toml` 的 `[tool.poetry] packages = [{include = "src"}]` 是讓 `src.xxx` 絕對匯入能運作的關鍵；容器內則改由 compose 設定的 `PYTHONPATH=/opt/airflow/`（Airflow）與 `PYTHONPATH=/app`（Cloud Run）達成同樣效果。
- `pyproject.toml` 的依賴清單與 `requirements.txt` 不會自動同步，兩者皆納入版控，改依賴時要一起更新。
