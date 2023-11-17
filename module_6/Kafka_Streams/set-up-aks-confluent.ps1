
# if you dont have helm uncomment the script for 
# your system

# for Windows:

# choco install kubernetes-helm

# for Linux:

# curl https://baltocdn.com/helm/signing.asc | gpg --dearmor | sudo tee /usr/share/keyrings/helm.gpg > /dev/null
# sudo apt-get install apt-transport-https --yes
# echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list
# sudo apt-get update
# sudo apt-get install helm


az aks get-credentials --resource-group rg-mm-westeurope --name aks-mm-westeurope
kubectl create namespace confluent
kubectl config set-context --current --namespace confluent
helm repo add confluentinc https://packages.confluent.io/helm
helm repo update
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes
kubectl apply -f confluent-platform.yaml
kubectl apply -f producer-app-data.yaml
# kubectl apply -f .\kstream-app.yaml