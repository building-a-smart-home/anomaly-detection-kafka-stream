from kafka import KafkaConsumer
import json
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.ensemble import IsolationForest
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import AuthenticationException  # Import the AuthenticationException

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# Kafka Consumer setup
consumer = KafkaConsumer(
    'temperature-data-stream',  # Replace with your topic name
    bootstrap_servers=[''],  # Replace with your broker's address
    group_id=None,  # Set a consumer group ID
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Ensure proper deserialization
    enable_auto_commit=True,
    auto_offset_reset='earliest',  # Start consuming from the earliest message
)

# Elasticsearch client setup
es = Elasticsearch(
  cloud_id="", # Replace with your Elasticsearch cluster URL
  basic_auth=("","") # Replace with your Elasticsearch API key
)

try:
    es.info()  # Verify connection
    print("Connected to Elasticsearch")
except AuthenticationException as e:  # Now it should recognize AuthenticationException
    print(f"Authentication failed: {e}")

# Initialize the Isolation Forest model for anomaly detection
model = IsolationForest(contamination=0.1)  # Expecting 10% anomalies

temperature_data = []

# Consume messages from Kafka topic
try:
    for message in consumer:
        logging.debug(f"Received message: {message.value}")  # Print the received message for debugging
        raw_message = message.value  # raw_message is already a dict, no need to decode
        try:
            # No need to decode since message.value is already a dict
            timestamp = raw_message["timestamp"]
            temperature = raw_message["temperature"]
        except KeyError as e:
            logging.error(f"Missing key in message: {e}")
            continue

        # Save the raw message to Elasticsearch in the "raw" index
        raw_doc = {
            'timestamp': timestamp,
            'raw_message': json.dumps(raw_message),  # Keep raw_message as a dict
            'received_at': datetime.now().isoformat()
        }
        res = es.index(index="raw-temperature-data", document=raw_doc)
        logging.debug(f"Raw message indexed: {res['_id']}")

        # Store data in a list (or you can store it in a more structured form like a DataFrame)
        temperature_data.append({"timestamp": timestamp, "temperature": temperature})

        # Check for enough data to analyze and perform anomaly detection
        if len(temperature_data) > 10:  # Example: perform detection after 10 data points
            df = pd.DataFrame(temperature_data)

            # Fit Isolation Forest model and predict anomalies
            df["anomaly"] = model.fit_predict(df[["temperature"]])

            # Mark anomaly points (-1 = anomaly, 1 = normal)
            df["anomaly"] = df["anomaly"].apply(lambda x: "Anomaly" if x == -1 else "Normal")

            # Print anomalies detected
            logging.debug("Detected Anomalies:")
            logging.debug(df[df["anomaly"] == "Anomaly"])

            # Save the anomaly data to Elasticsearch in "temperature-anomalies" index
            for _, row in df.iterrows():
                doc = {
                    'timestamp': row['timestamp'],
                    'temperature': row['temperature'],
                    'anomaly': row['anomaly'],
                    'created_at': datetime.now().isoformat()
                }

                # Index the document into Elasticsearch
                res = es.index(index="temperature-anomalies", document=doc)
                logging.debug(f"Document indexed: {res['_id']}")

            # Optionally, clear the data after each analysis or store it for historical analysis
            temperature_data = []  # Reset data after processing

            # Plot temperature data and anomalies
            plt.plot(df["timestamp"], df["temperature"], label="Temperature")
            plt.scatter(df["timestamp"], df["temperature"], c=df["anomaly"].apply(lambda x: 'r' if x == 'Anomaly' else 'g'),
                        label="Anomalies", zorder=5)
            plt.xlabel("Timestamp")
            plt.ylabel("Temperature (°C)")
            plt.title("Anomaly Detection in Temperature Data (Kafka Stream)")
            plt.legend()
            plt.show()

except Exception as e:
    logging.error(f"Error in consuming messages: {e}")
finally:
    consumer.close()  # Always close the consumer connection after processing
