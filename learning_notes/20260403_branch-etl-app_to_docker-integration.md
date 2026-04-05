# Procedures of Branch/etl-app to Branch/docker-integration
1. Merge the etl-app to docker-integration.
2. If no needed to utilize poetry in docker container, then just export the versions of tools what we install via poetry in the previous branch.
    ```
    # [Optional] If poetry-plugin-export has not installed yet.
    poetry self add poetry-plugin-export

    # Generate requirements.txt about the package dependency to reproduce the environment in branch 1 when pip installing the them in docker containers.
    # Abandon using poetry because it may make the size of images huge.
    poetry export -f requirements.txt --output requirements.txt --without-hashes
    ```
3.  Revise .env file
    ```
    MYSQL_HOST=localhost # change to service name by mysql container
    MYSQL_PORT=3306 # change to 3307 if need to connect with container and on-premise and if 3306 is already used for MySQL server on-premise.
    
    MYSQL_USER=root # change to service name by airflow container.  
    # Important: You cannot use root for MYSQL_USER because it will conflict with the built-in role when initializing MySQL container (root will be automatically created so we cannot add new user that is also named as root).

    MYSQL_PASSWORD= # password when connect with mysql container with airflow
    MYSQL_ROOT_PASSWORD= # Add new pwd for the root.

    REDIS_HOST=localhost # change to service name by mysql container
    REDIS_PORT=6379 # keep this if need to connect with container and on-premise.
    REDIS_PASSWORD= # keep this if need to connect with container and on-premise.

    GOOGLE_MAP_API_KEY= # keep this regardless of docerkization.

    ```
4. Initiate ./docker/Dockerfile.airflow
    ```
        FROM apache/airflow:3.1.7-python3.12

        # 不強制寫WORKDIR，因為會預設使用/opt/airflow
        # EXPOSE 8080不用寫，因為本就預設8080或是yml檔那邊會寫
        # 不需要寫COPY /dags、/task複製程式碼，因為啟動容器時會用mounting技術

        # 安裝基本系統工具
        # 如需要使用mysqlclient來連線到mysqlclient，需要安裝編譯mysqlclient時所需要的系統庫
        # RUN apt-get install -y --no-install-recommends \
        #     default-libmysqlclient-dev \
        #     pkg-config
        USER root
        RUN apt-get update && \
            apt-get install -y --no-install-recommends \
            build-essential \
            && apt-get clean && rm -rf /var/lib/apt/lists/*

        USER airflow

        COPY requirements.txt /requirements.txt
        RUN python -m pip install --no-cache-dir -U pip setuptools wheel && \
            python -m pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
    ```

5. Initiate ./docker/Dockerfile.streamlit
    ```
    FROM python:3.12

    # 設定工作目錄
    WORKDIR /app

    # 安裝基本系統工具
    # 如需要使用mysqlclient來連線到mysqlclient，需要安裝編譯mysqlclient時所需要的系統庫
    # RUN apt-get install -y --no-install-recommends \
    #     default-libmysqlclient-dev \
    #     pkg-config
    RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        && apt-get clean && rm -rf /var/lib/apt/lists/*

    # 複製依賴清單到/app/下
    COPY requirements.txt .

    # 升級 pip 並安裝套件
    RUN python -m pip install  --no-cache-dir -U pip setuptools wheel && \
        python -m pip install --no-cache-dir -r requirements.txt

    # 複製原始碼 (note: 這行是為了讓image沒有掛載時也能獨立運作，但是compose.yml中有寫會用volume覆蓋，所以實際上以yml描述為主)
    COPY ./src ./src
    COPY ./.streamlit ./.streamlit

    # Streamlit預設監聽8501，
    # 但因為以後要遷移streamlit到cloud run執行，為了配合cloud Run習慣，在啟動指令中指定監聽8080
    # Compose.yml要記得寫streamlit容器內8080映射到主機8080
    EXPOSE 8080

    # 啟動指令：--server.address=0.0.0.0 是容器化運行的關鍵、讓streamlit監聽所有網路介面
    CMD ["streamlit", "run", "src/app.py", "--server.port=8080", "--server.address=0.0.0.0"]
    ```

6. Initiate docker-compose.yml

7. Test building the containers.
    ```
        # Start running the docker daemon. Then:
        docker compose up -d --build

        # check docker containers is in "Up" or "(healthy)" status.
        docker ps

        # try to visit the following addresses:
        # streamlit pages
        http://localhost:8080

        # airflow UI
        http://localhost:8081
    ```

8. If successfully visited, then create the AIRFLOW DAGs in /dags
    - To well apply the retry mechanism of AIRFLOW, use class AirflowException:
    ```
        from airflow.exceptions import AirflowException

        ...
        ...
        try:
            # your codes
        except Exception as e:
            print(f"Error msg {e}")
            raise AirflowException
    ```
    - 
9. Trigger the DAGs
    - Keypoint: Let the Task returns shorter str, dict, list object since AIRFLOW utilizes it as x-com but strictly limit the length of x-com.
    - Task dependency shall be defined after instanced. E.g.: 
    ```
        # option 1
        task1_done = task1_func()
        task2_done = task2_func()
        task1_done >> task2_done

        # option 2
        task1_func()
        task2_func()
        task1_func() >> task2_func()
    ```
    - `Note`, some of SQL syntax may not compatible when running with python environment. For example:
        * DATE_FORMAT(d.accident_date, "%Y-%m") AS `accident_yearmonth`:  
        如果不是直接在MySQL環境下互動，只需打"%Y-%m"，若在MySQL環境下執行該函式，要打%%Y-%%m。
        
        * DELIMITER $$ 搭配 pymysql的cursor.execute()會失效，無法解析DELIMITER語法。

        * 使用AIRFLOW + pymysql來批次執行複數個SQL語句時，有3種方式，用途與效果不一樣:
            ```
                # 讀取整份sql腳本，回傳長文字，再使用sqlparse.format來移除註解後斷行，每行語句作為元素放入list，
                # 接著逐行執行。然而，sqlparse.format()處理分號的邏輯並不嚴謹，
                # 舉例而言，若腳本有STORED PROCEDURE，且CREATE PROCEDURE 函式名.....BEGIN下文中的語句有;符號，此時format()會把PROCEDURE切的太細碎，導致定義錯誤、執行錯誤。
                
                with open(file_path, mode="r") as f:
                    sql_content = f.read()

                # 使用 sqlparse 移除註解並格式化，若不移除而直接使用string的split，會導致MySQL執行錯誤。
                clean_sql = sqlparse.format(sql_content, strip_comments=True)

                # 分割成語句列表(sqlparse會自動處理分號、但不夠嚴謹)。
                list_of_sql_statements = sqlparse.split(clean_sql)

                # 移除空語句
                list_of_sql_statements = [stmt.strip() for stmt in list_of_sql_statements
                                           if stmt.strip()]
                print(f"去除註解後、清理SQL語句數量: {len(list_of_sql_statements)}")

                # 開始執行
                for line in list_of_sql_statements:
                    cursor.execute(line)
            ```

            ```
                # 直接執行整份sql file中的SQL腳本且不用預先移除註解，適合DDL，因為有SQL injection風險故不建議用在insert/update。
                from pymysql.contants import client.multi_statement

                conn = pymysql.connect(host=, port=, user=, 
                                        password=, database=,...., 
                                        client_flag=CLIENT.MULTI_STATEMENTS)  # 必須增加client_flag
                cursor = conn.cursor()
                cursor.execute(sql_content) # sql_content是一整份含有多個SQL語句的長文字腳本，當中的註解不會影響執行。
            ```
            ```
                # 參數化批量操作，較安全，適合insert/update DML，它一次處理多筆相同結構的資料，效率也比較好
                sql = "INSERT INTO users(name, age) VALUES(%s, %s)"
                data = [('Alice', 25), ('Bob', 30), ('Carol', 35)]
                cursor.executemany(sql, data)  # 自動執行三筆 INSERT
            ```
    - `Note`, 以AirFlow管理任務時，需注意try-except是否被swallowed
        * 以上寫法如果cursor.executemany()報錯，會進入print(f"Error......{e}")，然後rollback()後，函式正常結束，task也執行完成return None，AIRFLOW會標記Task為Success
            ```
                def a_func():
                    try:
                        conn = get_pymysql_conn_to_mysql(database)
                        if conn:
                            cursor = conn.cursor()
                            cursor.executemany(dml_str, df_fact_accident_env.values.tolist())
                            conn.commit()
                    except Exception as e:
                        print(f"Error on inserting into table, Error msg: {e}")
                        if conn:
                            conn.rollback()
                
                @task
                def task_a_func():
                    a_func()
                    return None

                task_a_func() 
            ```
        * 改善方式：
            ```
                def a_func():
                    try:
                        conn = get_pymysql_conn_to_mysql(database)
                        if conn:
                            cursor = conn.cursor()
                            cursor.executemany(dml_str, df_fact_accident_env.values.tolist())
                            conn.commit()
                    except Exception as e:
                        print(f"Error on inserting into table, Error msg: {e}")
                        if conn:
                            conn.rollback()
                        raise AirflowException("要raise才能讓Airflow捕捉有例外、並進入重試機制") # 方式1
                        raise AirflowFailException("要raise才能讓Airflow捕捉有例外且判定任務fail、不進入重試機制") #方式2
                
                @task
                def task_a_func():
                    a_func()
                    return None

                task_a_func() 
            ```
        * 核心概念：Airflow重試機制的觸發主要依賴未處理的Exception來向上傳播，任何形式的 exception swallowing 都會讓任務假成功。如果一定要寫try-except的話就要避免這件事發生，要確保raise Exception; 不然就是移除程式碼中的try-except、完全仰賴airflow來捕捉所有例外與重試機制。事實上，官方推薦後者。
        * 最需要保留try-except的情境是，當你需要確保與資料庫的連線關閉，需要寫:
            ```
                ...
                ...
                ...
                finally:
                    conn.close()
            ```