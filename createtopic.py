from kafka.admin import KafkaAdminClient, NewTopic

# Kafka server address (broker)
bootstrap_servers = ['']  # Replace with your broker address

# Initialize the Kafka Admin client
admin_client = KafkaAdminClient(
    bootstrap_servers=bootstrap_servers,
    client_id='kafka-python-admin'
)

# Define the topic parameters
topic_name = 'temperature-data-stream'  # Name of the topic you want to create
num_partitions = 2  # Number of partitions for the topic
replication_factor = 2  # Replication factor (how many copies of the topic are stored across brokers)

# Create the topic
topic = NewTopic(
    name=topic_name,
    num_partitions=num_partitions,
    replication_factor=replication_factor
)

# Create the topic on the Kafka cluster
admin_client.create_topics(new_topics=[topic], validate_only=False)

print(f"Topic '{topic_name}' created successfully!")
