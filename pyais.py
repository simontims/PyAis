import os
import json
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import requests
import logging
import pytz
import sys
sys.stdout.reconfigure(line_buffering=True)

# Configure timezone
LOCAL_TIMEZONE = pytz.timezone("Pacific/Auckland")

def get_local_time():
    """Get the current local time in the configured timezone."""
    return datetime.now(LOCAL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Environment variables
MQTT_SERVER = os.getenv("MQTT_SERVER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))  # Default to 1883 as it's a standard MQTT port
HA_SERVER_URL = os.getenv("HA_SERVER_URL")
HA_TOKEN = os.getenv("HA_TOKEN")
MQTT_TOPICS = os.getenv("MQTT_TOPICS").split(",")  # Comma-separated list of topics
DATA_FILE_PATH = "/data/mmsi_data.json"  # Hardcoded path for persistent storage

if not all([MQTT_SERVER, HA_SERVER_URL, HA_TOKEN, MQTT_TOPICS]):
    raise ValueError("Missing required environment variables: MQTT_SERVER, HA_SERVER_URL, HA_TOKEN, MQTT_TOPICS")

# Headers for Home Assistant API
HEADERS = {
    "Authorization": f"Bearer {HA_TOKEN}",
    "Content-Type": "application/json"
}

# Build the topic-to-sensor mapping dynamically
TOPIC_TO_SENSOR = {
    topic: f"{HA_SERVER_URL}/api/states/sensor.{topic.replace('/', '_')}"
    for topic in MQTT_TOPICS
}

# Storage for MMSI tracking
mmsi_data = {}

def load_mmsi_data():
    """Load MMSI data from a JSON file."""
    global mmsi_data
    if os.path.exists(DATA_FILE_PATH):
        try:
            with open(DATA_FILE_PATH, 'r') as file:
                raw_data = json.load(file)
                # Convert timestamp strings back to datetime objects
                mmsi_data = {
                    topic: [(entry['mmsi'], datetime.fromisoformat(entry['timestamp'])) for entry in entries]
                    for topic, entries in raw_data.items()
                }
                logger.info(f"Loaded MMSI data from {DATA_FILE_PATH}")
        except Exception as e:
            logger.error(f"Failed to load MMSI data: {e}")
    else:
        logger.info(f"No existing data file found at {DATA_FILE_PATH}, starting fresh.")

def save_mmsi_data():
    """Save MMSI data to a JSON file."""
    try:
        # Prepare data for JSON serialization
        raw_data = {
            topic: [{'mmsi': m, 'timestamp': t.isoformat()} for m, t in entries]
            for topic, entries in mmsi_data.items()
        }
        with open(DATA_FILE_PATH, 'w') as file:
            json.dump(raw_data, file)
        logger.info(f"Saved MMSI data to {DATA_FILE_PATH}")
    except Exception as e:
        logger.error(f"Failed to save MMSI data: {e}")

def post_to_home_assistant(sensor_url, name, mmsi, vessel_count):
    """Post the data to Home Assistant."""
    try:
        payload = {
            "state": "active",
            "attributes": {
                "name": name,
                "mmsi": mmsi,
                "vesselsInLastHour": vessel_count
            }
        }
        response = requests.post(sensor_url, headers=HEADERS, json=payload)
        response.raise_for_status()
        logger.info(f"Posted to Home Assistant: {sensor_url} {payload}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to post to Home Assistant: {e}")

def on_message(client, userdata, message):
    """Handle incoming MQTT messages."""
    # logger.info(f"Message received on topic {message.topic}: {message.payload.decode('utf-8')}")

    global mmsi_data

    topic = message.topic
    sensor_url = TOPIC_TO_SENSOR.get(topic)
    if not sensor_url:
        logger.warning(f"Received message on unconfigured topic: {topic}")
        return

    try:
        # Parse the incoming message
        data = json.loads(message.payload.decode("utf-8"))
        mmsi = data.get("mmsi")
        name = data.get("name")
        timestamp = datetime.now()

        logger.info(f"Received message: {name} ({mmsi}) on topic {topic}")

        if not mmsi or not name:
            logger.warning("Message missing required fields (name or MMSI), skipping.")
            return

        # Initialize tracking for this topic if not already present
        if topic not in mmsi_data:
            mmsi_data[topic] = []

        # Add the MMSI with a timestamp to the tracking list
        mmsi_data[topic].append((mmsi, timestamp))

        # Remove MMSIs older than 60 minutes
        cutoff = datetime.now() - timedelta(minutes=60)
        mmsi_data[topic] = [(m, t) for m, t in mmsi_data[topic] if t > cutoff]

        # Save updated MMSI data
        save_mmsi_data()

        # Calculate unique MMSIs
        unique_mmsis = {m for m, t in mmsi_data[topic]}
        vessel_count = len(unique_mmsis)

        # Post to Home Assistant
        post_to_home_assistant(sensor_url, name, mmsi, vessel_count)

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main():
    """Main function to set up MQTT client and listen for messages."""
    logger.info(f"Script started at {get_local_time()}")
    logger.info(f"Configured topics: {MQTT_TOPICS}")

    # Load MMSI data from file
    load_mmsi_data()

    client = mqtt.Client()
    client.on_message = on_message

    logger.info(f"Connecting to MQTT broker: {MQTT_SERVER}:{MQTT_PORT}")

    # Connect to MQTT broker
    client.connect(MQTT_SERVER, MQTT_PORT, 60)

    # Subscribe to all configured topics
    for topic in MQTT_TOPICS:
        client.subscribe(topic)
        logger.info(f"Subscribed to MQTT topic: {topic}")

    # Start the MQTT loop
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info("Exiting...")
        save_mmsi_data()
        client.disconnect()

if __name__ == "__main__":
    main()
