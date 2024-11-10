import json
from kafka import KafkaProducer
import time
import random
# Kafka settings
bootstrap_servers = ''  # Replace with your Kafka bootstrap servers
topic = 'temperature-data-stream'  # Replace with your Kafka topic

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializing data to JSON
)

# Function to create and send messages with anomalies
def produce_messages():
    try:
        for i in range(1, 41):  # Produce 40 messages for hourly data
            # Generate some normal and some anomalous data
            if i % 5 == 0:  # Inject an anomaly every 5th record
                temperature = 50 + random.uniform(5, 10)  # Anomalous temperature (e.g., very high)
                humidity = 60 + random.uniform(5, 10)  # Anomalous humidity
            else:
                temperature = 20 + random.uniform(-2, 2)  # Normal temperature (within a small range)
                humidity = 50 + random.uniform(-2, 2)  # Normal humidity

            message = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),  # Current timestamp in ISO format
                "temperature": temperature,  # Temperature with occasional anomalies
                "humidity": humidity  # Humidity with occasional anomalies
            }

            # Send the message to Kafka
            producer.send(topic, value=message)
            print(f"Sent message: {message}")

            # Optional: wait for a short time before sending the next message
            time.sleep(1)  # Adjust to simulate real-time production

    except Exception as e:
        print(f"Error producing message: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    produce_messages()
