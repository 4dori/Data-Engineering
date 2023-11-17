# Module 2: Spark SQL homework

In this module, se have explored Delta tables, the basics of Databricks, Databricks Notebook, Databricks CLI using Azure, PySpark, and SQL language. Also, we used Terraform to deploy environment in Azure cloud. 

The aim of the module is to learn how to analyze data in tables. 

# Given data in tables
1. Hotel_weather: It consist data that we created in module one
2. expedia: Data about hotel guests. 

# Tasks
1. Calculate Top 10 Hotels with max absolute temperature difference by month
2. Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.
3. For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.
4. For designed queries (tasks: 1, 2, 3) analyze execution plan
5. Deploy Databricks Notebook in Azure
6. Store final DataMarts in Azure gen 2 blob storage. 

# Deteailed description of the work done by me

1. Create Notebook in Databricks community edition. The language is python.
2. Create Cluster in Databricks. Insert Azure storage OAuth credentials in the spark environment.
2. pip install pydantic
3. Using pydantic extract credentials from the environment
4. Confugure spark session 
5. Import tables from Azure storage account 
6. Convert tables into Delta tables
7. Write Task 1, Task 2, and Task3. You can explore them in Databricks Notebook "/notebooks/MetadataCreate.py"
8. Store final tables in Azure gen 2 blob storage using abfs driver.
8. Setup and deploy infrastructure using terraform:
- craete .env file with OAuth credentials
- create "create-cluster-temp.json" file which comprises Databricks clusters characteristiks. 
- create "databricks.sh" file. It consist of theree blocks: export credentials into the environment, import authentication variables to json file, import authentication variables to main.tf (terraform), and create cluster and deploy databricks notebook. (for details check ./databricks.sh)
9. Run Databricks Notebook in Databricks.

# Screenshots of the outputs

Task 1
----
![First part](/Images/First_Part.png "First part")

Task 2
---
![Second part](/Images/Second_Part.png "Second part")

Task 3
---
![Third part](/Images/Third_Part.png "Third part")

Execution plan
---
![explain](/Images/explain.png "explain")

Persisted data in Azure
---
![Persisted data](/Images/Persisted_DataMart.png "Persisted data")

# Useful commands in databricks CLI

    databricks configure --token
    databricks clusters create
    databricks clusters get
    databricks clusters list
    databricks workspace import


