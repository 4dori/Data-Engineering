# export all variables from .env file
set -a
source .env
set +a

# in terraform check: vm_size = "Standard_D8_v3"
# use kubectl config view for server url and port
# I added "COPY ./src /opt/" to Dockerfile

spark-submit \
    --master k8s://bdcckk-iyuq4dfl.hcp.westeurope.azmk8s.io:443 \
    --deploy-mode cluster  \
    --conf spark.driver.memory=3G \
    --conf spark.executor.instances=6 \
    --conf spark.executor.memory=2G \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=4dori/sparkbasics:1.0.5 \
    --conf spark.kubernetes.driverEnv.AZURE_ACC_1=$AZURE_ACC_1 \
    --conf spark.kubernetes.driverEnv.AZURE_CLIENT_ID_1=$AZURE_CLIENT_ID_1 \
    --conf spark.kubernetes.driverEnv.AZURE_CLIENT_SECRET_1=$AZURE_CLIENT_SECRET_1 \
    --conf spark.kubernetes.driverEnv.AZURE_TENANT_ID_1=$AZURE_TENANT_ID_1     \
    --conf spark.kubernetes.driverEnv.AZURE_ACC_2=$AZURE_ACC_2 \
    --conf spark.kubernetes.driverEnv.AZURE_ACC_KEY_2=$AZURE_ACC_KEY_2 \
    --conf spark.executorEnv.AZURE_ACC_1=$AZURE_ACC_1 \
    --conf spark.executorEnv.AZURE_CLIENT_ID_1=$AZURE_CLIENT_ID_1 \
    --conf spark.executorEnv.AZURE_CLIENT_SECRET_1=$AZURE_CLIENT_SECRET_1 \
    --conf spark.executorEnv.AZURE_TENANT_ID_1=$AZURE_TENANT_ID_1    \
    --conf spark.executorEnv.AZURE_ACC_2=$AZURE_ACC_2 \
    --conf spark.executorEnv.AZURE_ACC_KEY_2=$AZURE_ACC_KEY_2 \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --py-files local:///opt/sparkbasics-1.0.0-py3.9.egg \
    local:///opt/main/python/__main__.py
