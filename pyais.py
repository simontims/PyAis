import os
import json
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import requests
import logging
import sys
sys.stdout.reconfigure(line_buffering=True)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Environment variables
MQTT_SERVER = os.getenv("MQTT_SERVER", "YOUR_MQTT_SERVER_IP")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "YOUR_MQTT_TOPIC")
HA_URI = os.getenv("HA_URI", "YOUR_HA_URL, ie http://hostname_or_ip:8123/api/states/sensor.yourSensorName")
HA_TOKEN = os.getenv("HA_TOKEN", "YOUR_BEARER_TOKEN_HERE")
DATA_FILE_PATH = os.getenv("DATA_FILE_PATH", "/data/mmsi_data.json")

# Headers for Home Assistant API
HEADERS = {
    "Authorization": f"Bearer {HA_TOKEN}",
    "Content-Type": "application/json"
}

# Storage for MMSI tracking
mmsi_data = []

def load_mmsi_data():
    """Load MMSI data from a JSON file."""
    global mmsi_data
    if os.path.exists(DATA_FILE_PATH):
        try:
            with open(DATA_FILE_PATH, 'r') as file:
                raw_data = json.load(file)
                # Convert timestamp strings back to datetime objects
                mmsi_data = [(entry['mmsi'], datetime.fromisoformat(entry['timestamp'])) for entry in raw_data]
                logger.info(f"Loaded {len(mmsi_data)} entries from {DATA_FILE_PATH}")
        except Exception as e:
            logger.error(f"Failed to load MMSI data: {e}")
    else:
        logger.info(f"No existing data file found at {DATA_FILE_PATH}, starting fresh.")

def save_mmsi_data():
    """Save MMSI data to a JSON file."""
    try:
        # Prepare data for JSON serialization
        raw_data = [{'mmsi': m, 'timestamp': t.isoformat()} for m, t in mmsi_data]
        with open(DATA_FILE_PATH, 'w') as file:
            json.dump(raw_data, file)
        logger.info(f"Saved {len(mmsi_data)} entries to {DATA_FILE_PATH}")
    except Exception as e:
        logger.error(f"Failed to save MMSI data: {e}")

def post_to_home_assistant(name, mmsi, vessel_count):
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
        response = requests.post(HA_URI, headers=HEADERS, json=payload)
        response.raise_for_status()
        logger.info(f"Posted to Home Assistant: {payload}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to post to Home Assistant: {e}")

def on_message(client, userdata, message):
    """Handle incoming MQTT messages."""
    global mmsi_data

    try:
        # Parse the incoming message
        data = json.loads(message.payload.decode("utf-8"))
        mmsi = data.get("mmsi")
        name = data.get("name")
        timestamp = datetime.now()

        logger.info(f"Received message: {name} ({mmsi})") 

        if not mmsi or not name:
            logger.warning("Message missing required fields (name or MMSI), skipping.")
            return

        # Add the MMSI with a timestamp to the tracking list
        mmsi_data.append((mmsi, timestamp))

        # Remove MMSIs older than 60 minutes
        cutoff = datetime.now() - timedelta(minutes=60)
        mmsi_data = [(m, t) for m, t in mmsi_data if t > cutoff]

        # Save updated MMSI data
        save_mmsi_data()

        # Calculate unique MMSIs
        unique_mmsis = {m for m, t in mmsi_data}
        vessel_count = len(unique_mmsis)

        # Post to Home Assistant
        post_to_home_assistant(name, mmsi, vessel_count)

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main():
    """Main function to set up MQTT client and listen for messages."""
    logger.info("Startup")

    # Load MMSI data from file
    load_mmsi_data()

    client = mqtt.Client()
    client.on_message = on_message

    logger.info(f"Connecting to MQTT broker: {MQTT_SERVER}:{MQTT_PORT}")

    # Connect to MQTT broker
    client.connect(MQTT_SERVER, MQTT_PORT, 60)

    # Subscribe to the topic
    client.subscribe(MQTT_TOPIC)
    logger.info(f"Subscribed to MQTT topic: {MQTT_TOPIC}")

    # Start the MQTT loop
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info("Exiting...")
        save_mmsi_data()
        client.disconnect()

if __name__ == "__main__":
    main()
