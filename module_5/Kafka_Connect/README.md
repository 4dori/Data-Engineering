# Module 4: Kafka Connect

In this module, we have explored:
- What is Kafka Connect and how to deploy it
- How to deploy Confluent Platform in Kubernetes
- How to set and configure Kafka Connect in Kubernetes
- How to create topic in kafka cluster
- How to create schema for the topic in schemaregistry
- How to upload data from Azure Blob Storage via Kafka Connect
- How to use transforms in Kafka Connect (MaskField)
- Repeat: Terraform, AZ-CLI, Kubectl, docker

# Tasks:

#### 1. Complete modules in Confluent Developer path

#### 2. Create Azure Blob Storage Kafka Connect image in Docker Hub
#### 3. Deploy AKS 
#### 4. Deploy Kafka in AKS
#### 5. Create Topic, Set Kafka Connect, Run The Job 

## 1. Completed modules in Confluent

### Confluent developer path certificates and screenshots

- Build an Apache Kafka Event Streaming Pipeline

![Event Streaming Pipeline](certificates/esp.png)

- Create an Event Streaming App with ksqlDB using Confluent Platform

![Event Streaming APP with ksqlDB](certificates/esa.png)

- Stream Processing with Kafka

![Stream Processing with Kafka](certificates/sp.png)


## 2. Create Azure Blob Sorage

1. Run docker
2. Sign in to docker
3. Go to ./connectors
4. run docker commands:
---
    docker build -f docker/Dockerfile .
    # check docker image
    docker image list
    docker image tag <image_id> <your_docker_hub_id>/my-azure-connector:1.0.0
    # upload image to Docker Hub
    docker push <your_docker_hub_id>/my-azure-connector:1.0.0

## 3. Deploy AKS

1. Create storage account in Azure portal
2. Write down Azure storage account credentials to main.tf file(./terraform/)
3. For deploying and running Kafka on AKS without delays and errors change "vm_size" to: "Standard_D8_v3".
4. Deploy AKS and storage via terraform:
---
    terraform init
    terraform plan -out terraform.plan
    terraform apply terraform.plan

## 4. Deploy Kafka in AKS

1. Install az cli (if you dont have)
2. Install helm (if you dont have)
3. Change Kafka Connect Image in confluent-platform.yaml: 
  from "my-azure-connector:1.0.0" to "<docker-id>/my-azure-connector:1.0.0"
3. Login to az cli.
3. run "set-up-aks-confluent.ps1" powerShell script file. Contents:
---
    # get AKS credentials to set up kubectl (Kubernetes cli)

    az aks get-credentials --resource-group rg-m5-westeurope --name aks-m5-westeurope

    # set up confluent platform in AKS

    kubectl create namespace confluent
    kubectl config set-context --current --namespace confluent
    helm repo add confluentinc https://packages.confluent.io/helm
    helm repo update
    helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes

    # deploy Kafka in AKS

    kubectl apply -f confluent-platform.yaml
    kubectl apply -f producer-app-data.yaml

  4. Wait till all pods are ready via kubectl command:
  ---
      kubectl get pods

  5. Upload topic file from "m11kafkaconnect" to "data"container


  ## 5. Create Topic, Set Kafka Connect, Run The Job

  1. Change "azure-source-cc-expedia.json" file (without comments):

    # write down your Azure Blob Storage credentials:
    # transforms made by Kafka Connecter before uploading messages into Kafka
    # mask time from the date field using MaskField transformer

      {
      "name" : "AzureBlobStorageSourceConnector",
      "config" : {
        "connector.class" : "io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnector",
        "tasks.max" : "1",
        "azblob.account.name" : "",
        "azblob.account.key" : "",
        "azblob.container.name" : "",
        "format.class" : "io.confluent.connect.azure.blob.storage.format.avro.AvroFormat",
        "confluent.topic.bootstrap.servers" : "kafka:9092",
        "confluent.topic.replication.factor" : "3",
        "transforms" : "MaskSensitiveFields",
        "transforms.MaskSensitiveFields.type" : "org.apache.kafka.connect.transforms.MaskField$Value",
        "transforms.MaskSensitiveFields.fields" : "date_time",
        "transforms.MaskSensitiveFields.replacement" : "0000-00-00 00:00:00"
      }
    }
2. Creaete "expedia-key.json" and "expedia-value.json" (you can check it in ./schema/ folder)
3. Run "start_connector.ps1" file to create Topic, schema, and connector:
---
    # create topic
    
    kubectl exec kafka-0 -- kafka-topics --create  --topic expedia --partitions 3 --replication-factor 3 --bootstrap-server kafka:9092 

    # copy schemas to schemaregistry

    kubectl cp .\schema\expedia-key.json confluent/schemaregistry-0:/opt
    kubectl cp .\schema\expedia-value.json confluent/schemaregistry-0:/opt

    # push shcemas to schemaregistry

    kubectl exec schemaregistry-0 -- curl -s -X POST -H 'Content-Type: application/json' --data @expedia-value.json http://schemaregistry:8081/subjects/expedia-value/versions
    kubectl exec schemaregistry-0 -- curl -s -X POST -H 'Content-Type: application/json' --data @expedia-key.json http://schemaregistry:8081/subjects/expedia-key/versions

    # configure and start connector for Azure Blob Storage

    kubectl cp .\connectors\azure-source-cc-expedia.json confluent/connect-0:/opt
    kubectl exec connect-0 -- curl -s -X POST -H 'Content-Type: application/json' --data @azure-source-cc-expedia.json http://localhost:8083/connectors

## Output data

- Messages from expedia topic. You can see masking in date_time field.

![Messages in expedia topic](images/datetime.png)

- Connector state

![Connector](images/connector.png)

- Value Schema

![Value schema](images/value-key.png)

- Key Schema

![Key schema](images/key.png)

## Useful commands

    kubectl logs <pod_name>
    kubectl exec <pod_name> -- <bash command>
    kubectl get pods




