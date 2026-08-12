# Taiwan Traffic Accident ETL × Night Market Accident Analytics Dashboard

![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=plastic&logo=python&logoColor=white)
![Poetry](https://img.shields.io/badge/deps-Poetry-60A5FA?style=plastic&logo=poetry&logoColor=white)
![ERD](https://img.shields.io/badge/ERD-dbdiagram%20chart-FFCE1B?style=plastic&logo=lucid&logoColor=white)
![MySQL](https://img.shields.io/badge/DB-MySQL-4479A1?style=plastic&logo=mysql&logoColor=white)
![Redis](https://img.shields.io/badge/cache-Redis-FF4438?style=plastic&logo=redis&logoColor=white)
![Airflow](https://img.shields.io/badge/orchestration-Apache%20Airflow%203-017CEE?style=plastic&logo=apacheairflow&logoColor=white)
![Streamlit](https://img.shields.io/badge/UI-Streamlit-FF4B4B?style=plastic&logo=streamlit&logoColor=white)
![Plotly](https://img.shields.io/badge/analytics-Plotly-%233F4F75.svg?style=plastic&logo=plotly&logoColor=white)
![Folium](https://img.shields.io/badge/map-Folium-77B829?style=plastic&logo=leaflet&logoColor=white)
![Tableau](https://img.shields.io/badge/BI-Tableau%20Public-E97627?style=plastic&logo=tableau&logoColor=white)
![Google Cloud](https://img.shields.io/badge/SaaS-GCP%20Cloud%20Platform-4285F4?style=plastic&logo=googlecloud&logoColor=white)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED?style=plastic&logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-000000?style=plastic&logo=githubactions&logoColor=white)
![Ruff](https://img.shields.io/badge/lint-Ruff-D7FF64?style=plastic&logo=ruff&logoColor=black)
![Claude Code](https://img.shields.io/badge/AI-Claude%20Code-D97757?style=plastic&logo=claudecode&logoColor=white)
![pytest](https://img.shields.io/badge/test-pytest-0A9EDC?style=plastic&logo=pytest&logoColor=white)
![Groq](https://img.shields.io/badge/AI-Groq-F55036?style=plastic&logo=groq&logoColor=white)  \
![data.gov.tw](https://img.shields.io/badge/data%20source-data.gov.tw-005CA9?style=plastic&logo=googledocs&logoColor=white)
![Open-Meteo](https://img.shields.io/badge/data%20source-Open--Meteo%20API-FF7F2A?style=plastic&logo=cloudflare&logoColor=white)
![Google Maps](https://img.shields.io/badge/data%20source-Google%20Maps%20API-34A853?style=plastic&logo=googlemaps&logoColor=white)
![Wikipedia](https://img.shields.io/badge/data%20source-Wikipedia-000000?style=plastic&logo=wikipedia&logoColor=white)

> 🌐 **English** ｜ [繁體中文版README](./README-zh-TW.md)


## About

By integrating three heterogeneous data sources, the geographic information of night
markets across Taiwan, years of traffic accident records, and weather observations, this
project builds a data-driven dashboard that quantifies traffic risk, helping both
government agencies and the general public locate and identify the high-risk hotspots of
what is known as "Taiwan, the pedestrian hell."

> This repo partially reuses [an earlier project built with classmates](https://github.com/CarlHung65/tjr104_t01).
The difference is that this repo aims to **practice the CI/CD and GCP Cloud Run Service
deployment that were never completed back then**, and to keep refactoring on top of it,
improving query and write performance as well as log quality.

**Implementation outline**: traffic accident records of severity A1／A2 from the Taiwanese
open data portal data.gov.tw, weather observations from Open-Meteo, and night market
information from Wikipedia and Google Maps are written into a MySQL star schema through 7
Airflow DAGs, with 1 more DAG writing into the Redis in-memory database, so that the
Streamlit frontend can present the analysis of **traffic safety factors and risk levels
around night markets**.

> [Live Demo](https://tjr104-tw-traffic-app-dev-219985522999.asia-east1.run.app/)

**How it works**:
The backend services (MySQL, Redis and Airflow) run on a GCP VM, started with Docker
Compose. The frontend Streamlit app is packaged as a standalone image and deployed to
Cloud Run Service, reaching MySQL and Redis on the VM's internal network through direct
VPC egress. Both GCP resources are deployed automatically via GitHub Actions. The ETL is
split into pure functions for the three stages `e_` extract, `t_` transform and `l_` load
under `src/task/`, while `dags/` is only responsible for wiring them together and deciding
schedules and dependencies.
The data model is a [star schema with five fact tables and four dimension tables](https://dbdiagram.io/d/new_Traffic-69a10021a3f0aa31e1405268);
the Chinese-to-English column mapping is centralized in `src/util/table_column_map.py`, and
the [ERD is available here](https://dbdiagram.io/d/new_Traffic-69a10021a3f0aa31e1405268).

**Design highlights**:
- The accident ETL uses **full load**. Every run fetches the entire dataset of the current
year, and the primary key is hashed from "day serial number, time and coordinates", which
keeps the write into MySQL idempotent. Backfilling past years is loaded in file batches
rather than pulled into memory all at once.

- The night market ETL also uses **full load**, taking "latitude, longitude and day of
week" as the unique key. Failures of the Google Places API are reported in the `status`
field of the response body rather than in the HTTP status code, so a custom exception class
distinguishes transient from permanent failures and retries only the transient ones, so
that quota is not wasted on retrying requests that are bound to fail.

- The weather ETL for accident locations uses **incremental load**, because the Open-Meteo
API has a request quota and the accident dataset is expected to reach 4 million rows, so a
full load on every run would waste quota for nothing. To make the incremental load work,
part of the stored weather data (the "observation point × month" pair) is encoded into the
GCS object path, and the path listing itself serves as the progress table: when a DAG
resumes or retries, it only requests the API for the missing paths. The transform stage
then applies **the same predefined grid-rounding function** on both sides, ensuring the
weather data and the accident data stored in MySQL map onto the same geographic grid at the
same granularity, so a JOIN between the two tables always has matches.
This design also reduces the number of API requests: roughly 300,000 to 400,000 accident
coordinates per year are reduced to a few thousand observation points after rounding, which
is equivalent to a few thousand geographic grids being requested.

- **The data layers consumed by the frontend**, such as the statistics of accidents around
night markets, are precomputed by DAG `d06`, which reads from MySQL, computes with pandas
and writes into Redis as the cache layer, so frontend pages only read the cache. The
balance between SQL and pandas was chosen against the machine specs allocated to this
project, **so that apart from the cold-start wait, every data read and render during the
lifetime of the Cloud Run Service stays within its memory quota and does not delay layer
rendering, giving a better user experience**.

## Feature

Each DAG is an independently triggerable unit. The DAG files only wire tasks together, and
the actual logic lives in the corresponding `e_`／`t_`／`l_` files under `src/task/`:

| Feature | Description | Schedule | Entry point |
| ---- | ---- | ---- | ------ |
| Table & full-year calendar creation | Creates the database and the [star-schema fact and dimension tables](https://dbdiagram.io/d/new_Traffic-69a10021a3f0aa31e1405268), then populates the accident-day dimension table | Manual, one-off | DAG [`d01`](./dags/d01_create_calendar_this_year.py) |
| Current-year accident ETL | Fetches A1／A2 severity accidents of the current year, loads three dimension tables and the accident master table in order, then writes the environment and party fact tables | 17:00 on the 1st, 10th and 20th of each month | DAG [`d02`](./dags/d02_track_recent_traffic_accidents.py) |
| Historical accident backfill | Same flow for 2021–2025; the 65 files of five years are loaded in batches, keeping memory pressure low | Manual | DAG [`d03`](./dags/d03_track_hist_traffic_accidents.py) |
| Mart layer rebuild | Runs the SQL under `mart_table_sql/` in order to rebuild the aggregates the frontend reads; any failing file rolls the whole batch back, so the mart layer never stops halfway through | 13:00 on the 15th of each month | DAG [`d04`](./dags/d04_analysis_pedestrian_accidents.py) |
| Night market ETL | Gets the night market list from Wikipedia, then fills in coordinates and opening hours via Google Maps | 11:00 on the 15th of each month | DAG [`d05`](./dags/d05_track_night_markets.py) |
| Redis precomputation | Statistics of accidents around night markets, computed ahead of time and cached for the frontend to read | 20:00 every 5 days | DAG [`d06`](./dags/d06_precompute_to_redis.py) |
| Current-year weather ETL | Fetched from Open-Meteo, staged as GCS Parquet and then loaded into MySQL, using "observation point × month" as the resume checkpoint | 07:00 on the 3rd and 18th of each month | DAG [`d07`](./dags/d07_track_recent_weather.py) |
| Historical weather backfill | Year-by-year backfill for 2021–2025 | Daily at 09:00, **pause manually once a DAG run has fully succeeded** | DAG [`d08`](./dags/d08_track_hist_weather.py) |

The frontend is a multi-page Streamlit app:

| Page | Description | File |
| ---- | ---- | ---- |
| Home | Navigation and entry points to the other pages | [app.py](./src/app.py) |
| Nationwide severity analysis | Night market accident severity filtered by region, city and time | [v_act1_all_accident.py](./src/pages/v_act1_all_accident.py) |
| City comparison | Safety benchmarking and year-over-year growth ranking by city and night market | [v_act1_city_accident.py](./src/pages/v_act1_city_accident.py) |
| Single night market AI diagnosis | Accident breakdown within a custom radius; the statistics are handed to Groq AI to generate protective recommendations | [v_act1_single_accident.py](./src/pages/v_act1_single_accident.py) |
| Before-and-after amendment analysis | Analytical dashboards built with Tableau | [v_act2_tableau.py](./src/pages/v_act2_tableau.py) |


## Tech. Stack

| Layer | Technology | Purpose |
| ----- | ---- | ---- |
| 01 Frontend | Streamlit, Plotly, Folium／streamlit-folium, Pandas, Tableau Public embedding | Multi-page dashboard, interactive charts and maps |
| 02 Ingestion & Parsing | Requests, BeautifulSoup4, Tenacity, data.gov.tw／Open-Meteo API／Google Maps API | Fetching, parsing and retrying across sources |
| 03 Database & Storage | MySQL 8.0, GCS, SQLAlchemy, PyMySQL, Google Cloud SDK | Relational database of accidents and weather (data layer); data lake as the staging layer for weather data |
| 04 Cache | Redis 7, Streamlit `cache_data` deep copy | Keeps page loads from hitting MySQL with heavy computation, ensuring low-latency responses |
| 05 Orchestration | Apache Airflow 3 (with LocalExecutor) | Manages DAG scheduling, dependencies and batching |
| 06 Auth & Permissions | GCP Application Default Credentials, Service Account, IAP tunnel, GitHub Secrets | GCS access and CI/CD identity verification |
| 07 Hosting & Deployment | - Backend: GCP VM with Docker containers <br>- Frontend: Cloud Run Service with Artifact Registry, direct VPC egress | Separate deployment of frontend and backend for read/write privilege separation |
| 08 AI | Groq API | Generative protective recommendations on the [single night market page](./src/pages/v_act1_single_accident.py) |
| 09 CI/CD & Version Control | - CI/CD: Git, GitHub Actions (`deploy-backend-vm` / `deploy-cloud-run`)<br>- Version control: Poetry | Automated deployment of the containers on the VM and of the Cloud Run Service |
| 10 Rate Limiting & Flow Control | - API request progress tracked by GCS path names, used as the basis for incremental load<br>- Streaming load of large accident datasets | Avoids a full reload when a task resumes after interruption; controls memory and I/O pressure |
| 11 Error Tracking & Logs | Custom `logger_crtx` | Readable logs and tracebacks both locally and in the cloud |
| 12 Quality Gate | pytest (315 tests), Ruff, pre-commit, CI test gate | Static checks and behavioural tests as a gate; a failing test blocks the deployment |
| 13 Availability & Recovery | Upsert, primary keys hashed from business logic | Keeps writes idempotent when tasks are retried or files are processed in batches |


## Architecture

![flowchart](./docs/architecture-readme.png)

## Project Structure

```plaintext
taiwan_traffic_accidents/          # project root
├── dags/                          # Airflow DAGs; wiring and scheduling only
│   ├── d01_create_calendar_this_year.py
│   ├── d02_track_recent_traffic_accidents.py
│   ├── d03_track_hist_traffic_accidents.py
│   ├── d04_analysis_pedestrian_accidents.py
│   ├── d05_track_night_markets.py
│   ├── d06_precompute_to_redis.py
│   ├── d07_track_recent_weather.py
│   └── d08_track_hist_weather.py
├── src/
│   ├── app.py                     # Streamlit entry point (home page)
│   ├── pages/                     #   v_*.py pages, must sit next to app.py
│   ├── task/                      # pure functions per ETL stage; the prefix is the stage
│   │   ├── e_*.py                 #   extract, returns a list of file paths
│   │   ├── t_*.py                 #   transform, returns a DataFrame
│   │   ├── l_*.py                 #   load, writes into MySQL
│   │   ├── create_*_tables.py     #   DDL for data layer tables
│   │   ├── mart_table_sql/        #   plain SQL scripts of the mart layer
│   │   ├── exec_mart_sql.py       #   runs the mart layer SQL scripts
│   │   └── core/                  #   Redis reads and rendering components for the frontend
│   │
│   └── util/                      # seven shared utility modules
│       ├── mysql_utils.py         #   MySQL connection, update and upsert helpers
│       ├── redis_utils.py         #   Redis connection, set cache, get cache
│       ├── gcs_utils.py           #   GCS connection, upload, download
│       ├── crawling_utils.py      #   general-purpose crawling helpers
│       ├── read_traffic_accident_file.py  # reading accident CSV files
│       ├── paths.py               #   keeps ETL scripts resolving paths correctly
│       ├── logger_crtx.py         #   shared logger, independent of the runtime
│       └── table_column_map.py    #   accident column name definitions
├── test/unit_test/                # 315 pytest tests (test_task_* / test_util_*)
├── docker/                        # Dockerfile.airflow, Dockerfile.streamlit
├── docker-compose.yml             # starts MySQL, Redis and Airflow (one command on the backend VM)
├── .github/workflows/             # deploy-backend-vm.yml, deploy-cloud-run.yml
├── .streamlit/config.toml         # Streamlit settings
├── docs/adr/                      # decision records (000N-<decision>.md) and execution summaries
├── data/                          # crawled accident CSV and night market JSON, all gitignored and bind-mounted at runtime
├── CONTEXT.md                     # glossary of the domain terms used in this project
├── CLAUDE.md                      # Claude Code project guide and decision index
├── pyproject.toml / poetry.lock   # Poetry dependencies
└── requirements.txt               # exported by Poetry for the Dockerfiles
```

## Get Started

### 1. Clone and set up

```bash
git clone --depth 1 https://github.com/Jessie-Lin-810630/taiwan_traffic_accidents_CICD_practice.git
cd taiwan_traffic_accidents_CICD_practice
```

Pick one of the two paths below:

#### Path A: just run the whole backend

Only Docker is required; Python and Poetry are not needed locally. After preparing the
`.env` file in the project root (see [section 2](#2-environment-variables)):

```bash
docker compose up -d --build
```

This starts MySQL, Redis and Airflow (scheduler / triggerer / api-server). The Airflow UI
is at `http://<host>:8081`.

> **The business database must be created manually**: the MySQL image in compose only
> creates one database on first start, and that slot is already taken by the Airflow
> metadata database. The business database for accident data has to be **created manually
> as root and granted** to `MYSQL_USER` before the DDL in DAG `d01` can run successfully.

#### Path B: reproduce the full development environment

For anyone who intends to modify the code. Python >= 3.12 and Poetry >= 2.x are
recommended.

```bash
poetry env use <path-to-python-3.12+>
poetry install
```

`pyproject.toml` already sets `packages = [{include = "src"}]`, so modules can be imported
absolutely as `src.xxx`.

---

### 2. Environment variables

```bash
cp .env.example .env      # fill in the real values after copying
```

15 variables in total:

- **9 are read by the Python code**: `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`,
  `MYSQL_PASSWORD`, `MYSQL_DATABASE`, `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`,
  `GOOGLE_MAP_API_KEY`, `GROQ_API_KEY`.
- **6 more are needed by docker compose to start the Airflow containers**:
  `MYSQL_AIRFLOW_DATABASE`, `MYSQL_ROOT_PASSWORD`, `AIRFLOW_SECRET_KEY`,
  `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_ADMIN_EMAIL`.

- GCS access does not go through environment variables; it uses Application Default
Credentials (`gcloud auth application-default login`, or a service account attached to the
deployment VM).

### 3. Run

```bash
# run a single ETL task
poetry run python -m src.task.e_crawling_traffic_accident

# start the frontend
poetry run streamlit run src/app.py

# tests (no config file, the path must be given)
poetry run pytest test/unit_test/
```

The mart layer SQL lives in `src/task/mart_table_sql/` and is normally executed as a batch
by DAG `d04`; to run it on its own, connect to MySQL and execute the `.sql` files in that
folder directly.

### 4. Deployment

Pushing to `main` or `UAT` triggers two workflows. **Both run the full test suite
first and stop before deploying if anything fails**:

- [`deploy-backend-vm.yml`](./.github/workflows/deploy-backend-vm.yml), which SSHes into the
VM through an IAP tunnel, builds the Docker images and starts the containers. The branch
pulled on the VM is whichever branch triggered the run.

- [`deploy-cloud-run.yml`](./.github/workflows/deploy-cloud-run.yml), which builds the
image, pushes it to Artifact Registry and deploys it to the Cloud Run Service.
  > `MYSQL_HOST` and `REDIS_HOST` on Cloud Run point to the internal IP of the VM, so
  > replacing the VM requires updating the workflow as well.

### 5. (Optional) Continue development with Claude Code

Start `claude` in the repo root and it automatically loads [`CLAUDE.md`](./CLAUDE.md) as
context (ETL layering rules, logger and exception handling conventions, the decision index
and the known state of the project).


## What's Next?

- [ ] **Run the CI test gate for real**: the gate is in place but has never executed on
  GitHub Actions; the first verification will happen on the next push to `main` or `UAT`.
