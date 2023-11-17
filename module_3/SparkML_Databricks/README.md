# Module 3: Spark ML

In this module, we have explored:
- Machine learning basics using python libraries like "Pandas", "Numpy", and "Sklearn" (Wich is now known as scikit-learn). 
- Learn how to visualize graphics using "Seaborn" and "Matplotlib".
- How to track the parameters and performance metrics using MLflow. 
- Productionize the model using "MLflow model serving"

# Given data and notebooks

- Databricks Notebook './notebooks/ML End-to-End Example.dbc'
- Wine quality CSV file "https://archive.ics.uci.edu/dataset/186/wine+quality" (We are using csv files from system datasets)

# Tasks 

Run Notebook file in databricks with modifications where needed

# Detailed description of the work done by me

1. Create main-temp.tf file in a project file:
- Copy code-block from main.tf file from the project
- Modify the subscription of the Databricks account. 
- Add storage account credentials as environment variables in 'backend "azurerm" block
2. Create .env file with needed credentials
3. Create terraform.sh file for changing credentials in main-temp.tf file and to create a proper main.tf file in terraform folder.
4. Create "create-cluster-temp.json" file with proper Databricks cluster characteristiks and environment variables.
5. Create "databricks.sh" file to substitute environment variables in temp file and to create new json file
6. Run terraform.sh file (you will need 'chmod a+x terrafrom.sh' command for execute permission)
7. Run:
---
    terraform init
    terraform plan -out terraform.plan
    terraform apply terraform.plan
8. After creating environment in Azure configure databricks cli using token.
9. Run databricks.sh file to create a cluster in databricks.
10. In databricks add:
---
    pip install mlflow
    pip install hepropt
    pip install xgboost

11. Change this lines:
---
    white_wine = pd.read_csv("/dbfs/FileStore/shared_uploads/arslan_mukhamatnurov@epam.com/winequality_white.csv", sep=';')
    red_wine = pd.read_csv("/dbfs/FileStore/shared_uploads/arslan_mukhamatnurov@epam.com/winequality_red.csv", sep=';')

### to:
---
    white_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-white.csv", sep=';')
    red_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-red.csv", sep=';')
12. Run the Notebook

# Screenshots of the outputs

Seaborn
---
![seaborn](/Images/seaborn.png "seaborn")

Pyplot
---
![pyplot](/Images/pyplot.png "pyplot")

Sainity check
---
![sainity-check](/Images/sainity-check.png "sainity-check")

Compare plot
---
![compare-plot](/Images/compare_plot.png "compare plot")

# Usefull commands

    databricks configure --token
    databricks fs -h
    databricks clusters create
    databricks clusters get
    databricks clusters list
    databricks workspace import
    chmod a+x file.sh
    template=$(cat main-temp.tf)
    parsed=$(envsubst <<< "$template")
    echo "$parsed" > ./terraform/main.tf

# comments

1. The Model serving part was difficult to implement due to subscription limits.
2. Some Python modules were not installed in the Python environment in Databricks.
3. Databricks notebooks with outputs can be found in the "./notebooks" directory.
4. To check the code, you may open a .ipynb file, and to view the outputs, you can check the .html or .ipynb file.