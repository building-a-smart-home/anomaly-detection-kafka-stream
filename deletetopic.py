from kafka.admin import KafkaAdminClient, NewTopic

# Connect to your Kafka cluster
admin_client = KafkaAdminClient(
    bootstrap_servers=[""],
    client_id='your_client_id'
)

# Delete the topic
admin_client.delete_topics([ "temperature-data-stream" ])
print("Topic deleted")
