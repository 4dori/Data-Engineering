
# Module 1: Spark basic homework

Module Overview
---------------

In this module, we have explored the basics of Spark and written ETL processes using PySpark, Docker, Azure ADLS Gen 2, Kubernetes, and Terraform.

ETL process description (in PySpark)
---------------------

1. Create a Spark session.
2. Authenticate to blobs using OAuth credentials.
3. Pull hotels and weather data from Azure blob storage.
4. Transform the data to the desired state
5. Join the final data into one object and push it to Azure blob storage

Steps to Deploy Spark job on Azure Kubernetes Service.
--------------------------------

1. Create the Azure environment (Storage account and AKS) using Terraform
2. Run "setup.py" file to create egg archive which will contain pachages, configuration and metadata.
3. Create docker image and push it to repository
4. Run the "spark-submit" command with proper options to deploy and run the job.

Project's link on GitHub
-------------------------

link

Description of the project
--------------------------

Main python code consists of three files: "\_\_main__.py" (main job file), "fns.py" (functions used to transform dataframe), and "sets.py" (OAuth credentials). There is a fourth file called ".env" that is used for persisting OAuth credentials. It is important to ensure that the ".env" file is added to the gitignore list to prevent sensitive information from being committed to the repository.

In src/test folder you can find Unit test codes. To run test you have to run run_test.sh file in bash. Before running test you have to install chispa and pytest packages from PyPi.

Unit test output
----------------

![Test results](/Images/Tests.jpg "Test results")

Spark job via Web Interfaces
---------------

![Spark Job 1](/Images/Spark%20jobs%20on%20K8s.jpg "Spark Job 1")

![Saprk Job 2](/Images/Spark%20job%20Details.jpg "Saprk Job 2")

Spark job result
----------------

![Spark job result](/Images/Job%20result.png "Job result")

Detailed description of the job done by me
------------------------------------------
1. Download the "m06_SparkBasics_Python_Azure" folder from the git.epam repository.
2. Install the latest Python and Python pip.
3. Install Java (openjdk 11).
4. Install 'virtuelenv' via pip.
5. Create and activate a virtual environment.
6. run "pip install -r requirements.txt".
7. run "pip install pydantic opencage".
- 'pydantic.BaseSettings' is used for protecting OAuth credentials.
- 'opencage' is used for retreiving longitude and latitude data using addresses.
8. Open Dockerfile in docker folder find lines starting with "wget -O $SPARK_HOME/jars/". Download jar files into pyspark installation folder.
8. Write your PySpark code:
- After completeng your transformation part, you need to persist the data in Azure blob storage. For this action, you have to run Terraform.
10. Running terraform:
- login to Azure using the command "az login"
- create a storage account and container in Azure
- inside the storage account, go to 'access keys' and get 'key1' key
- modify your main.tf file in terraform folder: add "storage_account_name", "container_name", "key", and "access_key" in kackend "azurerm". In AKS.default_node_pool change vm_size to "Standard_D8_v3"
- open terraform folder in PowerShell and run the following commands:\
     "terraform init" \
     "terraform plan -out terraform.plan", write any env name \
     "terraform apply terraform.plan"
- Go to Azure portal and check for the created resources (there should be around 10 new resources)
11. Go to Azure portal and take access key credentials from storage account (blob gen 2). Write them to 'spark.configs': \
 "spark.conf.set(f"fs.azure.account.key.{settings.AZURE_ACC_2}.dfs.core.windows.net", settings.AZURE_ACC_KEY_2)"
11. Modify Dockerfile in docker folder:
- COPY ./src /opt/
- add to pip3 install "pydantic" and "opencage"
12. Building the Docker image:
- go to project folder
- build docker image \
     $docker builds -f docker/Dockerfile .
- check images \
     $docker image list 
- create tag in docker repo: \
    $docker image tag \<image_ID> \<your_docker_hub_id>/sparkbasics:1.0.0
- upload image to docker hub:
    $docker push \<your_docker_hub_id>/sparkbasics:1.0.0
13. Run spark job in AKS using your docker image:
- run "spark.py" file with command: python3 setup.py bdist_egg
- Retrieve aks credentials using: \
     $az aks get-credentials --resource-group rg-\<env>-westeurope --name aks-\<env>-westeurope
- create account for spark-submit authentication: \
     $kubectl create serviceaccount spark \
    $kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
- activate your venv and run: \
  ./spark.sh \
  It has all OAuth credentials and 'spark-submit' command with necessary options.

Writing tests for job's functions
--------------------------------

- pip install pytest
- Go to test folder
- Create conftest.py file
- Write code for spark session in "def spark():" and use decorator: @pytest.fixture
- Create test_\<your_functions_file>.py
- Write tests for each function
- Create "run_tests.sh" file in src folder
- Write there: 
python -m pytest test/test_*.py --disable-pytest-warnings;
- run .sh file in bash

Modifying project files
-----------------------

- docker/Dockerfile:
    - add COPY src/ /opt/
    - add to pip3 install "pydantic" and "opencage"
- terraform/main.tf:
    - add to terraform.backend:
        - "storage_account_name" 
        - "container_name" 
        - "key"
        - "access_key" 
    - modify rsource"aks".default_node_pool.vm_size: "Standard_D8_v3"
- requirements.txt add:
    - pydantic>=1.10.7
    - pytest>=7.3.1
    - opencage>=2.2.0
    - chispa>=0.9.2

Addidtional commands
--------------------

- docker run -it \<image-id> bash  \
- kubectl get pods \
- kubectl get pods \<pod_name> \
- kubectl describe pods \
- kubectl describe pods \<pod_name> \
- kubectl logs \<pod_name> \
- kubectl port-forward \<pod_name> 4040:4040 \
- kubectl config view

Addres for web interface:
--------------

- localhost:4040

Attention!!!
-------------------
- terraform did not work on wsl
- 'fns.py' and 'sets.py' files did not work in AKS. Thats why I used them in one \_\_main__.py file
- Before writing PySpark job download jars from spark website. Look for jars in dockerfile
- I could not connect to Azure blob storage via Service principal. I used blobs 'Access key'.

Running job local
-----------------

- run requirements.txt
- download jars
- create .env file in src/main/python/ and add OAuth credentials
- run src/main/python/\_\_main__.py







