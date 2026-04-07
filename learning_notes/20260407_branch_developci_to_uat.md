# Procedures of Branch/develop/CI to Branch/UAT
1. Create a repository in the GCP Artifact Registry.
    ```
        repository name: traffic-web-service
        region: asia-east1
        format: Docker
    ```
2. Establish the VPC connector and obtain the access to backend VM. 
    - Target: UseServerless VPC Access to allow `Cloud Functions` and `App Engine` apps to access resources in a VPC network using those resources’ private IPs.  
    可以直接從 Serverless 環境（如Cloud Run、App Engine 或 Cloud Functions）連接到自己專案的虛擬私有雲（VPC）網路。(https://medium.com/@kellenjohn175/explanation-gcp-infrastructure-serverless-vpc-access-實現serverless-與-vpc-資源之間的互連-b373ff6a9928)
    - Step 1: Go to GCP console -> VPC Network -> Serverless VPC Access
        * note: You may need to enable the serverless VPC access API if you never do it before.
    - Step 2: Click 'create connector', enter as follows:
        * name: streamlit-vpc-connector
        * region: asia-east1
        * network: default (or, the network where your VM is located)
        * subnet: custom IP range , 10.8.0.0/28
        * The creating task may take 3-5 minutes once you click 'create'.
        ** note: network means '請把 Cloud Run 的流量導入名為 default 的那個 VPC 網路中。一般來說，VM開啟時若沒有特別設定，VM也會是在default網路中。使用network設定代表將connector掛載到default網路下，這樣cloud run 才能看到這台VM'
        ** subnet網段必須與VM internal IP不衝突。Connector就像是一個「轉接頭」，當Cloud Run要發起請求時，它會臨時占用這段網段裡的 IP 來當作發信人地址。
    
    - Step 3: Go to firewall -> Create firewall `rule`
        * rule name: allow-cloud-run-to-db
        * rule description: Allows cloud-run to database in vm through vpc connector.
        * target tags: Specified target tags or all instance in the network.
        * Source IP4 ranges: must the same as the subnet of VPC connector.
        * TCP port: 3306 (to MySQL), 6379 (to Redis)
        * Click 'create'
3. Initiate the deploy-cloud-run.yml.
    - Key idea:
        * 地端 (GitHub)：將程式碼包裝成 Docker Image。
        * 中轉站 (Artifact Registry)：儲存這個 Image。
        * 終點站 (Cloud Run)：拉取 Image 並透過 VPC Connector 連向 VM的internal IP。
    - Key Jobs:
        * Job-1: Build and Push Image to Artifact Registry
        * Job-2: Deploy to Cloud Run based on the image on Artifact Registry

4. Revise the docker-compose.yml from branch 3.
    - Step 1: Make sure the MySQL and Redis will keep listening 0.0.0.0 after docker containers starts.
        * For MySQL: offical image usually apply 0.0.0.0 as default, so no need to further revised. Just ensure opening the ports (e.g. 3306:3306).

        * For Redis: newly add `--protected-mode no` in the `command=`. 
    - Step 2: Create new SA authorized "artifact registry writer", "artifact registry reader", and "cloud run admin" roles, or add these three roles to the existing SA since branch 3.

5. Push the branch UAT to Github. Then Github Actions will autmatically trigger the jobs in deploy-cloud-run.yml.