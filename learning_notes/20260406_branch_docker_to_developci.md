# Procedures of Branch/feature/docker-integration to Branch/develop/CI
    - GitHub Actions 機器人邏輯：  
    Checkout：把你push到remote的代碼（不含 .env）抓 (pull) 下來。  
    SSH：讀取Github Secrets上的GCP_SSH_key，用它登入 GCP VM。  
    Generate .env file：把 GitHub Secrets 的值寫進 VM 裡的 .env 檔。  
    Docker Compose：執行 up --build。Docker 容器啟動過程會抓取那個剛生成的 .env，完成資料庫連線。

1. Go to Actions of your repo.
2. Find the block titled 'Simple Workflow' and click 'configure'.
3. Merge branch2 to branch3 as the structural plan in README.md.
4. Create .github/workflows/deploy.yml
    ``` 
        cd <專案根目錄>
        mkdir ./github/workflows

        touch ./github/workflows/deploy.yml
    ```
5. Go the your github repository, then click settings -> Secrets and Variables -> Actions
6. Add required environment variables to Github repository secrets: GCP_SSH_USER, GCP_SSH_KEY, MYSQL_HOST, MYSQL_PASSWORD, ....; Usually, the secrepts contains the variables in the .env file that comes from the previous branch 'dockerization', and the credentials to connect with GCP VM. The value of some variables will be generated at the following procedures, so you can just add the variable names in Github secrets but leave their values blank temporarily.

7. Create a GCP VM with "e2-standard-2" (2 vCPU + 8GB RAM), and Spot.

8. Generate a set of public/private keys on-premise.
    ```
        ssh-keygen -f ~/.ssh/<key組名稱> -C "<金鑰的識別名>"
    ```
9. Put the private key onto Github Secrets.
    ```
        # get the private key content.
        cat ./ssh/<key組名稱>

        # You will see the key content likes...
        -----BEGIN OPENSSH PRIVATE KEY----- 
        ...
        ...
        ...
        -----END OPENSSH PRIVATE KEY-----

        # copy the whole string from `----BEGIN` to `----END OPENSSH PRIVATE KEY----`. DO NOT miss any letter or redundant spaces when copy.

        # paste the string to `GCP_SSH_KEY on Github repository secrets.
    ```
10. Store the public key in VM.
    ```
        # get the public key content.
        cat ./ssh/<key組名稱>.pub

        # You will see the key content likes...
        ssh-.......... <key組識別碼>-----

        # copy the whole string and paste it to the GCP VM.
    ```
    - In this step, you can also check if the ENABLE-OS LOGIN is set FALSE from the page MetaData. ENABLE-OSLOGIN=TRUE means that authorization to log in VM is controlled by IAM rather than SSH key. 

11. Store the <key組識別碼> in 'GCP_SSH_USER' on Github repository secrets.

12. [IF not use IAP] find the external IP of VM and store it in 'GCP_VM_EXTERNAL_IP' on Github repository secrets.
    ```
        gcloud config set project <GCP專案id>

        gcloud compute instances list # Then you will get the exterinal IP of the VM that just created.
    ```

13. [If use IAP] generate a Service Account and its JSON KEY. Store the JSON KEY on Github repository secrets.  
    - 原理是GitHub Actions拿著 SA Key 讓gcloud看知道他(這台虛擬機)有權走IAP服務找到VM 的IP，然後拿著SSH Key登入VM。

14. Login the VM.
    ```
        gcloud compute ssh <GCP_SSH_USER>@<GCP_VM_NAME> --zone="<ZONE_NAME>" --tunnel-through-iap --project="<PROJECT_ID>" --ssh-key-file="~/.ssh/<PRIVATE_KEY_NAME>"
        
        e.g.:
        gcloud compute ssh Deploy-to-GCP-VM-practice-cicd-tjr104@practice-cicd-tjr104 --zone="asia-east1-c" --tunnel-through-iap --project="causal-inquiry-484423-e7" --ssh-key-file="~/.ssh/github_actions_tw_traffic"
    ```
15. Manually create new folder of which the path is same as that described in deploy.yml
    ```
        # e.g.:
        mkdir ~/${{ secrets.GCP_SSH_USER }}/taiwan_traffic_accidents
        # taiwan_traffic_accidents <--- Usually this is the same as the repo name on github.
    ```
16. [Optional] Install git in VM.
    ```
        sudo apt-get update
        sudo apt-get install -y git
    ```
17. Generate the SSH key (the 2nd set) in VM.  
    - 原理是機器人要在VM裡面找到 Deploy Key，拿著它向github伺服器要代碼。
    ```
        # 同step 15, gcloud compute ssh進入VM，然後：
        ssh-keygen -t ed25519 -C <你自己的gmail帳號>@gmail.com

        cat ~/.ssh/id_ed25519 # 取得公鑰
    ```
18. Paste the ssh `public key` in Github Settings -> Security and quality -> Deploy keys -> Add deploy key

19. Test git commands can be used in VM.
    ```
        git init
        git clone https://github.com/<github帳號>/<github repo name>.git
        git pull origin <分支名>

        # make sure the SSH_USER is assigned as the owner of the project folder, and belongs to the group with identical name to SSH_USER.
        sudo chown -R <SSH_USER_NAME>:<SSH_USER_NAME> <folder_name>

        # check the own is correctly changed:
        ls -la <folder_name>

        # for example:
        sudo chown -R cicd-practice:cicd-practice taiwan_traffic_accidents
        ls-la taiwan_traffic_accidents        
    ```
20. Install docker
    ```
        sudo apt update
        sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

        # inspect docker is successully installed 
        docker --version

        # inspect docker can be executed without 'sudo'
        docker ps
        
        # if 'docker ps' returns Permission denied, then add the SSH_USER to docker Group
        sudo usermod -aG docker <SSH_USER_NAME>

        # exit and login VM again
        # check 'docer ps' again

        # [optional] Let the Actions can play as `root` and use `sudo` if need.
        sudo -i
        echo "<SSH_USER_NAME> ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/<SSH_USER_NAME>
        sudo ls /root
    ```

21. git push the branch develop/CI to remote, the github actions will start to test the deployment job automatically.