# Procedures of Branch/feature/docker-integration to Branch/develop/CI
1. Go the your github repository, then click settings -> Secrets and Variables -> Actions
2. Add four new repository secrets: GCP_VM_external_IP, GCP_SSH_USER, GCP_SSH_KEY, MYSQL_ROOT_PASSWORD
3. Go to Actions of your repo.
4. Find the block titled 'Simple Workflow' and click 'configure'.
5. Merge branch2 to branch3 as the structural plan in README.md.
6. Create .github/workflows/deploy.yml
    ``` 
        cd <專案根目錄>
        mkdir ./github/workflows

        touch ./github/workflows/deploy.yml
    ```
7. Split the information in .env file line by line, and paste them to Github Repository Secrets. Like what we perform at step 2.
    - GitHub Actions 機器人邏輯：  
    Checkout：把你push到remote的代碼（不含 .env）抓下來。  
    SSH：登入 GCP VM。  
    Generate .env file：把 GitHub Secrets 的值寫進 VM 裡的 .env 檔。  
    Docker Compose：執行 up --build。Docker 會抓取那個剛生成的 .env，完成資料庫連線。

8. Create a GCP VM with "e2-standard-2" (2 vCPU + 8GB RAM), and Spot.

9. Generate a set of public/private keys on-premise.
    ```
        ssh-keygen -f ~/.ssh/<key組名稱> -C "<金鑰的識別名>"
    ```
10. Put the private key onto Github Secrets.
    ```
        # get the private key content.
        cat ./ssh/<key組名稱>

        # You will see the key content likes...
        -----BEGIN OPENSSH PRIVATE KEY----- 
        ...
        ...
        ...
        -----END OPENSSH PRIVATE KEY-----

        # copy the whole string from `----BEGIN` to `----END OPENSSH PRIVATE KEY----`

        # paste the string to `GCP_SSH_KEY on Github repository secrets.
    ```
11. Store the public key in VM.
    ```
        # get the public key content.
        cat ./ssh/<key組名稱>.pub

        # You will see the key content likes...
        ssh-.......... <key組識別碼>-----

        # copy the whole string and paste it to the GCP VM.
    ```
12. Store the <key組識別碼> in 'GCP_SSH_USER' on Github repository secrets.

13. Find the external IP of VM and store it in 'GCP_VM_EXTERNAL_IP' on Github repository secrets.
    ```
        gcloud config set project <GCP專案id>

        gcloud compute instances list # Then you will get the exterinal IP of the VM that just created.
    ```
14. Generate SA and JSON KEY. Store the JSON KEY on Github repository secrets.  
    - 原理是GitHub Actions拿著 SA Key 讓gcloud看知道他有權走IAP服務找到VM門牌，然後拿著SSH Key登入VM。
15. Login the VM.
    ```
        gcloud compute ssh Deploy-to-GCP-VM-practice-cicd-tjr104@practice-cicd-tjr104 --zone="asia-east1-c" --tunnel-through-iap --project="causal-inquiry-484423-e7" --ssh-key-file="~/.ssh/github_actions_tw_traffic"
    ```
16. Manually create new folder of which the path is same as in deploy.yml
    ```
        # e.g.:
        mkdir /home/${{ secrets.GCP_SSH_USER }}/taiwan_traffic_accidents
    ```
17. [Optional] Install git in VM.
    ```
        sudo apt-get update
        sudo apt-get install -y git
    ```
18. Generate the SSH key (the 2nd set) in VM.  
    - 原理是VM得拿著 Deploy Key 向 GitHub 伺服器要代碼。
    ```
        # gcloud compute ssh進入VM
        ssh-keygen -t ed25519 -C <你自己的gmail帳號>@gmail.com

        cat ~/.ssh/id_ed25519 # 取得公鑰
    ```
19. Paste the ssh `public key` in Github Settings -> Security and quality -> Deploy keys -> Add deploy key

20. test git commands can be used in VM.
    ```
        git init
        git remote add origin git@github.com:<你的github帳號>/<你的repo專案名>.git
        git pull origin <分支名>develop/CI
    ```
21. install docker
    ```
        sudo apt-get update 
        sudo apt-get install -y docker.io
        sudo systemctl start docker
        sudo systemctl enable docker

        # inspect docker is successully installed 
        docker --version

        # inspect docker can be executed without 'sudo'
        docker ps
        
        # if 'docker ps' returns Permission denied, then:
        sudo usermod -aG docker $USER

        # exit and login VM again
        # check 'docer ps' again
    ```

22. git push the branch develop/CI to remote, the github actions will start to test the deployment job automatically.