
# create topic

kubectl exec kafka-0 -- kafka-topics --create  --topic expedia --partitions 3  --replication-factor 3  --bootstrap-server kafka:9092 

# copy schemas to schemaregistry

kubectl cp .\schema\expedia-key.json confluent/schemaregistry-0:/opt
kubectl cp .\schema\expedia-value.json confluent/schemaregistry-0:/opt

# push shcemas to schemaregistry

kubectl exec schemaregistry-0 -- curl -s -X POST -H 'Content-Type: application/json' --data @expedia-value.json http://schemaregistry:8081/subjects/expedia-value/versions
kubectl exec schemaregistry-0 -- curl -s -X POST -H 'Content-Type: application/json' --data @expedia-key.json http://schemaregistry:8081/subjects/expedia-key/versions

configure and start connector for Azure Blob Storage

kubectl cp .\connectors\azure-source-cc-expedia.json confluent/connect-0:/opt
kubectl exec connect-0 -- curl -s -X POST -H 'Content-Type: application/json' --data @azure-source-cc-expedia.json http://localhost:8083/connectors
