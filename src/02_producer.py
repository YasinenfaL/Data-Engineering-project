"""
IoT data producer for Kafka.
Generates synthetic IoT data or reads from sample data file.
"""
import json
import time
import random
import os
from typing import Dict, Union, List
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "iot_data")
SLEEP_MIN_SECONDS = 0.5
SLEEP_MAX_SECONDS = 2.0

# Sample data file path
SAMPLE_DATA_PATH = "src/sample_data.json"

def create_kafka_producer() -> KafkaProducer:
    """Creates and returns a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_random_iot_data() -> Dict[str, Union[str, float]]:
    """Generates random IoT data."""
    return {
        "deviceId": f"device_{random.randint(1, 5)}",
        "temperature": round(random.uniform(20.0, 42.0), 2),
        "humidity": round(random.uniform(30.0, 80.0), 2),
        "timestamp": time.time(),
        "location": f"server_room_{random.randint(1, 3)}",
        "status": random.choice(["active", "warning", "critical"])
    }

def load_sample_data() -> List[Dict]:
    """Loads sample data from JSON file."""
    try:
        with open(SAMPLE_DATA_PATH, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Sample data file not found at {SAMPLE_DATA_PATH}")
        return []
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {SAMPLE_DATA_PATH}")
        return []

def produce_data(producer: KafkaProducer, use_sample_data: bool = False):
    """Produces IoT data to Kafka topic.
    
    Args:
        producer: Kafka producer instance
        use_sample_data: If True, uses data from sample file; if False, generates random data
    """
    if use_sample_data:
        sample_data = load_sample_data()
        if not sample_data:
            print("Falling back to random data generation...")
            use_sample_data = False
    
    print(f"Starting data production to topic: {TOPIC_NAME}")
    try:
        while True:
            if use_sample_data:
                # Continuously cycle through sample data
                for data in sample_data:
                    data["timestamp"] = time.time()  # Update timestamp
                    producer.send(TOPIC_NAME, value=data)
                    print(f"Sent sample data: {data}")
                    time.sleep(random.uniform(SLEEP_MIN_SECONDS, SLEEP_MAX_SECONDS))
            else:
                data = generate_random_iot_data()
                producer.send(TOPIC_NAME, value=data)
                print(f"Sent generated data: {data}")
                time.sleep(random.uniform(SLEEP_MIN_SECONDS, SLEEP_MAX_SECONDS))
    except KeyboardInterrupt:
        print("\nStopping data production...")
    except Exception as e:
        print(f"Error during data production: {e}")
    finally:
        producer.close()
        print("Producer closed.")

def main():
    """Main function to set up producer and start data production."""
    producer = create_kafka_producer()
    # Set to True to use sample data from file, False for random generation
    produce_data(producer, use_sample_data=True)

if __name__ == "__main__":
    main() 