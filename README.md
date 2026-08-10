# Traffic Accident ETL & Visualization Project

# Purpose
This repository partially `refers to my previous contribution` (repository link: https://github.com/CarlHung65/tjr104_t01) where I collaborated with classmates. The difference between these two repositories is, the purpose of initiation of this repository is to `practice` `CI/CD concepts` and `GCP cloud run` which had not learned and implemented in the past.

# Business goal
This repository demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline for Taiwan traffic accident data, featuring a frontend dashboard built with Streamlit. The ultimate goal is to deploy a web microservice on Google Cloud Platform (GCP) using Cloud Run (https://streamlit-service-219985522999.asia-east1.run.app). The web service provided by GCP cloud run will communicate with backend GCP VM.

# Workflows in each branch
Based on the CI/CD concepts, the project is organized into five sequential branches. Each branch represents a specific milestone in the development lifecycle, including its core functions, environment, and deliverables. Detailed are listed as follows.

## Branch 1, name: "feature/etl-app"
1. core func.: Establish the core ETL logic and ensure data insights are correctly visualized via Streamlit. This branch uses Poetry for dependency management and virtual environment control. All scripts must pass unit tests before being merged into subsequent branches.
2. environment: python + MySQL on premises
3. planned directories:
    ```
        my_project/
            ├── src/
            │   ├── pages/                       # streamlit 頁面（強制要跟app.py入口同一層）
            |   ├── tasks/                       # ETL 任務核心邏輯
            |   |     ├── e_*.pu, t_*.py, l_*.py # 後端資料庫ETL任務
            |   |     ├── mart_table_sql/        # ETL完成後相關純MySQL查詢語句
            |   |     └── core/                  # 存放過渡到前端展示時，pages/需要的UI元件、前端視圖運算等
            |   ├── util                         # ETL 核心邏輯
            │   └── app.py                       # Streamlit run入口
            ├── tests/
            │   └── test_transform.py            # Unittest 測試檔
            ├── sandbox/
            │   └── miscellaneous.py             # 放置一些想做版控、但現在暫時用不到的函式
            ├── .env                             # 本地環境變數(e.g, DB_HOST=localhost)，
            |                                      在github remote branch上會以.env.example示範
            |
            ├── pyproject.toml                   # Poetry 設定
            ├── poetry.lock                      # 精確版本鎖定
            └── .gitignore                       # 存放不需要trace的檔案、檔案類型
    ```
4. To make sure the modules in src/ can be successfully imported, remember to include /src/ in packages in pyproejct.toml. Then no requirement to add 'sys.path.insert' in the header of every python scripts.
    ``` #for example,add:
            packages = [{include = "src"}]'
    ```

## Branch 2, name: "feature/docker-integration"
1. core func.: Containerize MySQL, Streamlit, and Apache Airflow. Airflow is utilized to schedule and automate ETL processes. This stage focuses on ensuring seamless communication and networking between containers.
2. sources: merged from Branch 1 and pyproject.toml. ``Any modifications to database connections or service networking are handled in this branch.``
3. planned directories:
    ```
        my-project/
            ├── dags/                       # + 存放Airflow DAGs
            ├── src/
            |   ├── pages/                  # (From Branch 1)
            |   ├── tasks/                  # (From Branch 1)
            |   ├── util/                   # (From Branch 1)
            │   └── app.py                  # (From Branch 1)
            ├── sandbox/                    # (From Branch 1)
            ├── .env                        # (Revised from Branch 1)容器化環境變數
            |                                 (e.g, DB_HOST=容器名稱)，在github remote branch上
            |                                 會以.env.example示範
            |
            ├── pyproject.toml              # (From Branch 1)
            ├── poetry.lock                 # (From Branch 1)
            ├── .gitignore                  # (From Branch 1)
            ├── docker/                     # + 容器定義
            │   ├── Dockerfile.airflow
            │   └── Dockerfile.streamlit
            ├── docker-compose.yml          # + 一鍵啟動所有容器
            └── requirements.txt            # + 執行poetry export產出
    ```
## Branch 3, name: "develop/CI"
1. core func.: Implement GitHub Actions to automate the building process of the docker containers in remote VM on GCP.
2. sources: some components from Branch 2.
3. planned directories:
    ```
        my-project/
            ├── .github/                    # + GitHub Actions自動化腳本
            │   └── workflows/
            │       └── deploy.yml          # + 測試在GCP VM上building container
            ├── dags/                       # (From Branch 2)
            ├── src/                        # (From Branch 2)
            |   ├── pages/
            |   ├── tasks/
            |   ├── util/
            │   └── app.py
            │
            ├── .env                        # (From Branch 2，在github remote branch上
            |                                  會以.env.example示範)
            ├── .gitignore                  # (From Branch 2)
            ├── docker/                     # (From Branch 2)
            │   ├── Dockerfile.airflow
            │   └── Dockerfile.streamlit
            ├── docker-compose.yml          # (From Branch 2)
            └── requirements.txt            # (From Branch 2)
    ```
## Branch 4, name: "UAT"
1. core func: A branch to create production-similar environment, aiming at check successfully deploying the AirFlow, MySQL and Redis on a GCP VM instance, and deploing Streamlit container on cloud run.
2. sources: Merged from develop/CI after all CI/CD checks pass.
3. directories:
    ```
        my-project/
            ├── .github/
            │   └── workflows/
            │       ├── deploy-backend-vm.yml   # + Revised from Branch 3
            |       └── deploy-cloud-run.yml    # + 測試 VPC connector access
            |                                     與cloud run service可運行。
            |
            ├── dags/                           # (From Branch 3)
            ├── src/                            # (From Branch 3)
            |   ├── pages/
            |   ├── tasks/
            |   ├── util/
            │   └── app.py
            │
            ├── .env.example                # + Revised from Branch 3, environment variables in this
            |                                  example are truely managed by Github secret or
            |                                  GCP secret manager rather than .env file
            |
            ├── .gitignore                  # (From Branch 3)
            ├── docker/                     # (From Branch 3)
            │   ├── Dockerfile.airflow
            │   └── Dockerfile.streamlit
            ├── docker-compose.yml          # + Revised from Branch 3; Separate AirFlow standalone mode to
            |                                 three containers, Scheduler、Trigger、api-server(webserver)
            |
            └── requirements.txt            # (From Branch 3)
    ```
## Branch 5, name: "main/production"
1. core func: Production-ready branch for stable service.
2. sources: All the components from branch 4.
3. directories:
    ```
        my-project/
            ├── README.md                       # Introduce the structure and user guide of this repository.
            ├── .github/
            │   └── workflows/
            │       ├── deploy-backend-vm.yml   # A CD workflow to deploying the backend computing services
            |       |                             on a GCP VM.
            |       └── deploy-cloud-run.yml    # A CD workflow to deploying the frontend demonstation to
            |                                     a GCP cloud run.
            |
            ├── dags/                           # AirFlow DAGs scheduling ETL pipeline in backend VM.
            ├── src/                            # All the required scripts & functions before orchestrated
            |   |                                 to an organized ETL data pipeline by AirFlow
            |   |
            |   ├── pages/                      # Frontend web pages via python-streamlit
            |   ├── tasks/                      # Tasks constributing DAGs
            |   ├── util/                       # Miscellaneous python functions without intact
            |   |                                 business logics but repeatedly called by tasks/
            |   |
            │   └── app.py                      # Fronted home page via python-streamlit.
            |
            ├── .streamlit/                     # Configuration changes about streamlit.
            |
            ├── .env.example                # Environment variables required to CD workflow and ETL
            |                                 datapipeline. In this example, they are truely managed by
            |                                 Github secret or GCP secret manager rather than an .env file.
            |
            ├── .gitignore                  # Untracked file name/type during development.
            |
            ├── docker/                     # Recipes defining customized images of AirFlow & Streamlit
            │   ├── Dockerfile.airflow
            │   └── Dockerfile.streamlit
            ├── docker-compose.yml          # Build and start MySQL, Redis and AirFlow containers
            |                                 (Scheduler + trigger + api-server(webserver))
            |                                 in backend GCP VM.
            |
            └── requirements.txt            # Defining package dependency when initiaing the containers.
                                              This contents of this doc will be copied when initiating
                                              containers by folloing the docker/Dockerfile.
    ```

# How to reproduce the development environment?
1. Pull the branch 1 `feature/etl-app`. Suggests to use python >=3.12 and poetry >=2.x at your local end.
3. Initiate the virtual environment by using poetry.
    ```
        cd <your專案根目錄>
        poetry env use [path_to_python>=3.12]
        poetry install
    ```
2. Always run under Project Root Directory (專案根目錄), then
    ```
        # Execute pure python scripts for ETL:
            poetry run python -m src.tasks.e_crawling_...
            poetry run python -m src.tasks.l_.....

        # Execute pure python scripts for frontend analysis:
            poetry run python -m src.cores.c_data_services

        # Execute the SQL srcipts to create the table of analysis results:
            You should have a MySQL server in your VM or local end first,
            and connect to the server and execute the statements in 'mart_table_sql/analysis_overview_pedestrian_accidents.sql'.
            In the branch feature/etl-app, statements are usually saved in pure .sql file; However, when running, either by GUI (e.g. workbench) or sqlalchemy ORM is okay.
            These SQL statements will be integrated into a new DAG in the next branch so the sqlalchemey module will be introduced then.

        # Execute the streamlit:
            poetry run streamlit run src/app.py
    ```
