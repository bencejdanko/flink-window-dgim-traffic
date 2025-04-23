import time
import random
import json
from datetime import datetime

# Probability of switching state (0 -> 1 or 1 -> 0)
# Lower value means longer bursts of activity or inactivity
SWITCH_PROBABILITY = 0.1

# Initial state (start with inactive)
current_state = 0

# Kafka producer setup
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_data_stream():
    """Generates a stream simulating network packets with burstiness."""
    global current_state
    while True:
         # Decide whether to switch state based on the probability
         if random.random() < SWITCH_PROBABILITY:
              current_state = 1 - current_state  # Flip the state

         packet = current_state
         # Create JSON message with value and timestamp
         message = {
             "value": str(packet),
             "timestamp": datetime.now().isoformat()
         }
         print(f"Generated packet: {message}")
         producer.send('network_traffic', message)

         # Simulate variable packet arrival time (Gaussian distribution around 0.5s)
         # Adjust mean (0.5) and standard deviation (0.1) as needed
         sleep_time = random.gauss(0.5, 0.1)
         # Ensure sleep time is not negative or too small
         sleep_time = max(0.05, sleep_time)
         time.sleep(sleep_time)

if __name__ == "__main__":
    print("Starting network traffic simulation (Ctrl+C to stop)...")
    try:
        generate_data_stream()
    except KeyboardInterrupt:
        print("Stopping traffic generator...")
    finally:
        producer.close()