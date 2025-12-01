import csv
import json
import time
from kafka import KafkaProducer

# --- CONFIGURATION -------------------------------------------------------------------
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'raw_clickstream'

# --- File Path ---
CSV_FILE_PATH = 'data/events.csv'   

# Delay in seconds to simulate real-time stream. Use 0.01 for faster streaming.
STREAM_DELAY = 0.1 
# -------------------------------------------------------------------------------------


# value_serializer converts Python dictionary to a JSON string (bytes)
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting Kafka Producer to {TOPIC_NAME} on {KAFKA_BROKER}...")

# Read the CSV file and publish records
try:
    with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
        # Use DictReader to read each row as a dictionary (key=header, value=cell)
        csv_reader = csv.DictReader(file)
        
        # We skip the header row, DictReader handles it
        
        for i, row in enumerate(csv_reader):
            # The 'row' is a dictionary: {'timestamp': '...', 'visitorid': '...', ...}
            
                
            # Send the data. We use 'visitorid' as the Kafka message key 
            # This ensures all events for the same user go to the same partition, 
            # which is an optimization for sessionization later.
            future = producer.send(
                topic=TOPIC_NAME, 
                key=str(row['visitorid']).encode('utf-8'), # Key must be bytes
                value=row                                  # Value is a dictionary, serialized by value_serializer
            )
            
            if (i + 1) % 1000 == 0:
                print(f"Published {i + 1} records. Last event: {row['event']} for visitor {row['visitorid']}")

            # Pause to simulate real-time ingestion
            time.sleep(STREAM_DELAY)

except FileNotFoundError:
    print(f"ERROR: CSV file not found at {CSV_FILE_PATH}. Please check the path and file name.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    # Ensure all messages are sent before exiting
    producer.flush() 
    producer.close()
    print("\nKafka Producer finished and closed.")