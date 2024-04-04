import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configuration
kafka_topic = 'CidrTest'
bootstrap_servers = ['localhost:9092']  # Adjust if your Kafka server has a different address
message_rate = 1  # Messages per second

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def generate_mock_interaction():
    """Generate a mock user interaction data."""
    user_id = random.randint(1, 10000)
    ip_address = f"{random.randint(1, 255)}.{random.randint(1, 255)}." \
                 f"{random.randint(1, 255)}.{random.randint(1, 255)}"
    timestamp = datetime.now().isoformat()
    event_type = random.choice(['page_view', 'add_to_cart', 'purchase'])
    product_id = random.randint(1, 100)

    return {
        'user_id': user_id,
        'ip_address': ip_address,
        'timestamp': timestamp,
        'event_type': event_type,
        'product_id': product_id
    }

def produce_messages(rate):
    """Produce messages to Kafka at a specified rate."""
    while True:
        interaction_data = generate_mock_interaction()
        producer.send(kafka_topic, interaction_data)
        print(f"Produced: {interaction_data}")
        time.sleep(1 / rate)

if __name__ == '__main__':
    try:
        produce_messages(message_rate)
    except KeyboardInterrupt:
        print("Stopping producer...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()
