#export all authentication variables from .env file
set -a
source .env
set +a

#import authentication variables to json file 
template=$(cat create-cluster-temp.json)
parsed=$(envsubst <<< "$template")
echo "$parsed" > create-cluster.json

#import authentication variables to main.tf (terraform)
template=$(cat main-temp.tf)
parsed=$(envsubst <<< "$template")
echo "$parsed" > ./terraform/main.tf

#create cluster and deploy databricks notebook
databricks clusters create --json @create-cluster.json
databricks workspace import --language PYTHON --overwrite ./notebooks/Simulation.py /Users/arslan_mukhamatnurov@epam.com/simulation.py
databricks workspace import --language PYTHON --overwrite ./notebooks/Streaming.py /Users/arslan_mukhamatnurov@epam.com/streaming.py