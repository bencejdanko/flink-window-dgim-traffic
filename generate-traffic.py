import time
import random
import json
from datetime import datetime
import math

# Kafka producer setup
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Configuration for varying probability ---
# How often the probability of generating '1' changes (in seconds)
# Adjust this based on your DGIM window size. Changes should occur
# within a fraction of the window duration to be noticeable.
PROBABILITY_CHANGE_INTERVAL = 15 # e.g., Change probability every 15 seconds

# Range of probabilities for generating '1'
MIN_PROBABILITY = 0.1  # Low traffic density
MAX_PROBABILITY = 0.8  # High traffic density

# Initial probability
current_probability_of_one = MIN_PROBABILITY
last_probability_change_time = time.time()
# --- End Configuration ---


def generate_data_stream():
    """Generates a stream simulating network packets with varying probability of '1's."""
    global current_probability_of_one, last_probability_change_time

    print(f"Starting with probability: {current_probability_of_one:.2f}")

    while True:
        current_time = time.time()

        # --- Update probability periodically ---
        if current_time - last_probability_change_time > PROBABILITY_CHANGE_INTERVAL:
            # Switch between MIN and MAX probability
            if current_probability_of_one == MIN_PROBABILITY:
                current_probability_of_one = MAX_PROBABILITY
            else:
                current_probability_of_one = MIN_PROBABILITY
            print(f"--- Changed probability of '1' to: {current_probability_of_one:.2f} at {datetime.now().isoformat()} ---")
            last_probability_change_time = current_time
        # --- End Update probability ---

        # Generate packet based on current probability
        if random.random() < current_probability_of_one:
            packet = 1
        else:
            packet = 0

        # Create JSON message with value and timestamp
        message = {
            "value": str(packet), # Ensure value is 0 or 1 for DGIM
            "timestamp": datetime.now().isoformat()
        }
        # print(f"Generated packet: {message}") # Optional: print every packet
        producer.send('network_traffic', message)

        # Simulate variable packet arrival time (Gaussian distribution around 0.02s for ~50 msg/sec)
        # Adjust mean (0.02) and standard deviation (0.005) as needed
        sleep_time = random.gauss(0.02, 0.005)
        # Ensure sleep time is not negative or too small
        sleep_time = max(0.001, sleep_time)
        time.sleep(sleep_time)

if __name__ == "__main__":
    print("Starting network traffic simulation with varying probability (Ctrl+C to stop)...")
    print(f"Probability will switch between {MIN_PROBABILITY} and {MAX_PROBABILITY} every {PROBABILITY_CHANGE_INTERVAL} seconds.")
    try:
        generate_data_stream()
    except KeyboardInterrupt:
        print("\nStopping traffic generator...")
    finally:
        if producer:
            producer.close()
            print("Kafka producer closed.")
