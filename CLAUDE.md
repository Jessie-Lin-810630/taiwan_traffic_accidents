# CLAUDE.md

給 Claude Code（claude.ai/code）看的專案指引。

**這份檔案只寫「該遵守什麼」與「去哪裡看為什麼」，不複述論證。**
決策的完整脈絡在 `docs/adr/`：決策文是 `000N-<決策>.md`，執行報告是
`000N-執行摘要-<主題>.md`。領域術語表在 `CONTEXT.md`。

## 專案概述

臺灣交通事故的 ETL + Streamlit 視覺化。後端（MySQL / Redis / Airflow）跑在 GCP VM
的 Docker Compose 上；前端 Streamlit 以獨立 image 部署到 Cloud Run，透過 direct VPC
egress 連回 VM 內網存取 MySQL 與 Redis。

資料模型是星狀綱要，五張事實表配四張維度表，中英欄位對照集中在
`src/util/table_column_map.py`。

## 常用指令

**所有指令都要在專案根目錄執行** —— 模組以 `src.xxx` 絕對匯入，靠 CWD 或
`PYTHONPATH` 解析。

```bash
poetry install                                    # Poetry 是開發端的真實來源
poetry run python -m src.task.e_crawling_traffic_accident   # 單獨跑一個 ETL 任務
poetry run streamlit run src/app.py               # 啟動前端
poetry run pytest test/unit_test/                 # 測試（無設定檔，要指定路徑）
docker compose up -d --build                      # 後端整套（需要根目錄有 .env）
```

依賴變動後必須重新匯出，否則容器裝到的版本會與本機不一致：

```bash
poetry export -f requirements.txt --output requirements.txt --without-hashes
```

Airflow UI 在 `http://<host>:8081`。

## 架構要點

### ETL：`e_` / `t_` / `l_`

`src/task/` 下每個檔案是一個 ETL 階段的純函式，檔名前綴即階段 ——
`e_` 抓取（回傳檔案路徑 list）、`t_` 清洗（回傳 DataFrame）、`l_` 載入（寫 MySQL）。
另有無前綴的工具型 task（建表、執行 mart SQL）。

`dags/dNN_*.py` 只負責串接。**改 ETL 邏輯改 `src/task/`，改排程與相依改 `dags/`。**

三條規矩：

- 讀事故 CSV 一律走 `read_traffic_accident_file()`，不要自己 `pd.read_csv`（ADR-0010）
- CSV 缺欄是資料來源的正常演進，記 `warning` 並繼續；**空 `pathlist` 相反，一律拋出**（ADR-0003）
- 事實表的載入以檔案批次為單位，避免 VM 上 OOM（ADR-0015）

### 天氣 ETL

OpenMeteo → GCS Parquet → MySQL。與其他 pipeline 的差別是**有外部額度限制**，
且 **GCS 的路徑就是抓取進度表**（ADR-0013）。經緯度兩側都要走同一支網格函式
（ADR-0012）—— 只要一側算法不同，merge 會一列都對不上且不報錯。

`d07` 定期跑，`d08` 是一次性回補、補完後手動 pause。

### 前端

`src/app.py` → `src/pages/v_*.py` → `src/task/core/`（`c_data_service` 業務運算、
`c_db` 純查詢、`c_ui` 共用元件）。

- **重運算由 DAG 預先算好進 Redis，前端只讀** —— 前端不做重運算
- **查詢的粒度要與使用它的粒度一致**（ADR-0009）：批次服務就整批一次查詢，
  迴圈裡不得再出現查詢
- **減量要在 SQL 完成，不是撈回來再用 pandas 篩**（ADR-0016）
- 座標一律走 bind parameter，不 f-string 內插

### logger 與例外處理

```python
from src.util.logger_crtx import get_logger
logger = get_logger(__name__)
```

五條規則，各有 ADR 背書，**在 `src/` 與 `dags/` 內皆已無例外**（由 AST 稽核把關）：

1. 不轉換成 `AirflowException`，一律原樣 `raise`（ADR-0001）
2. 不把故障吞成「正常但空」的回傳值（ADR-0003）
3. `exc_info=True` 只用在例外停止傳播之處 —— 有 `raise` 就不帶（ADR-0003）
4. `finally` 只負責釋放資源，裡面不放 `return`（ADR-0005）
5. 會被重試的失敗記 `warning`，不會被重試的記 `error`（ADR-0006）

`src/util/` 與 `src/task/` 全部模組**都能在無 Airflow 的環境被匯入**（地端、pytest、
Cloud Run），由測試把關。`dags/` 底下照常 import airflow。

### 共用工具（`src/util/`）

七支模組各有明確職責、有測試把關：MySQL 存取、Redis、GCS、通用抓取能力、路徑、
事故 CSV 讀取契約、logger。

- **MySQL 取連線一律用 `get_engine_to_mysql()`，不要自己 `create_engine()`，
  也不要 `dispose()` 它**（ADR-0004）
- **抓取層只放「換一個資料來源仍然成立」的東西**，站台專屬的解析規則屬於
  `src/task/e_*.py`（ADR-0006）
- **路徑一律從 `src.util.paths` 取，不要用 `Path().resolve()`**（ADR-0007）
- 共用工具保持單一語意：事務邊界、切批是呼叫端的業務決定，不做進 `src/util/`

### 環境變數

`.env`（gitignore）供 docker compose 使用，`.env.example` 是範本。
部署時由 GitHub Secrets 注入。

共 15 個，分成互不重疊的兩群：**Python 讀取的 9 個**（MySQL / Redis 連線資訊與
API key，散在 `mysql_utils` / `redis_utils` / `e_crawling_nightmarket` 的模組層）與
**只有 docker compose 用的 6 個**（Airflow 啟動與自己的 metadata database）。
後者 Python 從不讀取，但**不可刪除** —— 「Python 不讀取」與「可以刪除」是兩件事。

其中四個必填項缺少時會在使用的那一刻拋出（ADR-0008）。GCS 不使用環境變數，
走 Application Default Credentials。

### CI/CD

推 `main` 或 `UAT` 會觸發兩條 workflow（`**.md` 與 `docs/**` 的變動會被忽略）：

- `deploy-backend-vm.yml` —— 經 IAP tunnel SSH 進 VM，重建 compose
- `deploy-cloud-run.yml` —— build image 推 Artifact Registry，`gcloud run deploy`

兩條都以 `test` job 為前置，**測試沒過就不部署**（ADR-0017）。VM 上拉取的分支由觸發
的分支決定（`${GITHUB_REF_NAME}`），不寫死任何一支。Cloud Run 上的 `MYSQL_HOST` /
`REDIS_HOST` 是 VM 的內網 IP，改 VM 要同步改 workflow。

## 決策索引

| ADR | 主題 |
| --- | --- |
| 0001 | 不使用 `AirflowException`，一律原樣拋出 |
| 0002 | upsert 邏輯集中於單一 module |
| 0003 | 快取層不再吞噬例外 |
| 0004 | MySQL Engine 以資料庫名為鍵的單例快取 |
| 0005 | `finally` 只負責釋放資源 |
| 0006 | 抓取層區分暫時性與永久性故障 |
| 0007 | 路徑以專案根為基準而非工作目錄 |
| 0008 | 必填設定在使用時驗證 |
| 0009 | 預計算的查詢粒度以批次為單位 |
| 0010 | 事故 CSV 的讀取契約集中於單一 module |
| 0011 | 天氣 ETL 併入既有存取介面 |
| 0012 | 氣象網格精度 |
| 0013 | 天氣抓取的斷點以觀測點乘月為單位 |
| 0014 | 事故主鍵由事故內容決定，而非單次 run 的排序名次 |
| 0015 | 事故事實表的載入以檔案批次為單位 |
| 0016 | 熱點圖的減量在 SQL 完成，而非 pandas |
| 0017 | 部署以測試通過為前提 |

## 已知狀態

- **前端已上線 Cloud Run**（dev 環境），首頁 OOM 已修復（ADR-0016），
  記憶體設定仍在觀察期
- **天氣 ETL 已上線** —— `d07`／`d08` 已在 VM 上跑過真實資料，
  `fact_hourly_weather` 已有內容
- **`get_accident_hotspots()` 尚無呼叫者**，新查詢未在真實資料上驗證過
  （ADR-0016 執行摘要第四章）
- `test/unit_test/` 現有 **315 個測試**，全部走 mock，不需要 MySQL／Redis／網路／
  環境變數。命名慣例：測 `src/task/*.py` 用 `test_task_*.py`、測 `src/util/*.py` 用
  `test_util_*.py`；測試函式名用中文，**每個測試都要有 docstring 寫出「釘住的是什麼」**
- ETL 產出落在 `data/raw` 與 `data/processed`（皆在 `.gitignore` 內，compose 有掛載）
- `src/task/` 與 `src/task/core/` 有 `temp_try_*.py` 暫存檔，非正式流程的一部分
- `pyproject.toml` 與 `requirements.txt` 不會自動同步，改依賴時要一起更新 ——
  CI 的 test job 以 `requirements.txt` 安裝，不同步時會在部署前被擋下
- **CI 測試 gate 已在真實環境驗證兩次**：第一次擋下 `ModuleNotFoundError`、未部署，
  修法後第二次兩條 workflow 全綠（ADR-0017 執行摘要第四章）

## Agent 工具設定

Skill 實體放在 `.agents/skills/`（**納入版控**），`.claude/skills/` 只是 symlink 且
`.claude` 已被 gitignore。新增 skill 後要確認 `.agents/` 有進 commit。

重構分析工具鏈的入口是 `improve-codebase-architecture`（只能由使用者輸入 `/` 觸發）。
**每個候選的產出是兩份文件**：決策文（動工前）與執行摘要（七章）。
**下一輪的候選來自上一輪摘要的第五、七章**，這是流程能接續的關鍵。

**ADR 生效後不再改動** —— 實作期的決定與偏離寫進執行摘要，不回頭改決策文。
