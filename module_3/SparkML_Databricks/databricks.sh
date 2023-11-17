#export all authentication variables from .env file
set -a
source .env
set +a

#import authentication variables to json file 
template=$(cat create-cluster-temp.json)
parsed=$(envsubst <<< "$template")
echo "$parsed" > create-cluster.json

#create cluster and deploy databricks notebook
databricks clusters create --json-file create-cluster.json
databricks workspace import --language PYTHON --overwrite './notebooks/ML End-to-End Example.py' /Users/arslan_mukhamatnurov@epam.com/module_3.py
