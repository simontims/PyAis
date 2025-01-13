import os
import json
import paho.mqtt.client as mqtt
import time
from datetime import datetime, timedelta
import requests

# Environment variables
MQTT_SERVER = os.getenv("MQTT_SERVER", "192.168.1.45")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "ais/data")
HA_URI = os.getenv("HA_URI", "http://192.168.1.45:8123/api/states/sensor.AISReceived")
HA_TOKEN = os.getenv("HA_TOKEN", "YOUR_BEARER_TOKEN_HERE")

# Headers for Home Assistant API
HEADERS = {
    "Authorization": f"Bearer {HA_TOKEN}",
    "Content-Type": "application/json"
}

# Storage for MMSI tracking
mmsi_data = []

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
        print(f"Posted to Home Assistant: {payload}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to post to Home Assistant: {e}")

def on_message(client, userdata, message):
    """Handle incoming MQTT messages."""
    global mmsi_data

    try:
        # Parse the incoming message
        data = json.loads(message.payload.decode("utf-8"))
        mmsi = data.get("mmsi")
        name = data.get("name")
        timestamp = datetime.now()

        if not mmsi or not name:
            print("Message missing required fields (name or MMSI), skipping.")
            return

        # Add the MMSI with a timestamp to the tracking list
        mmsi_data.append((mmsi, timestamp))

        # Remove MMSIs older than 60 minutes
        cutoff = datetime.now() - timedelta(minutes=60)
        mmsi_data = [(m, t) for m, t in mmsi_data if t > cutoff]

        # Calculate unique MMSIs
        unique_mmsis = {m for m, t in mmsi_data}
        vessel_count = len(unique_mmsis)

        # Post to Home Assistant
        post_to_home_assistant(name, mmsi, vessel_count)

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    """Main function to set up MQTT client and listen for messages."""
    client = mqtt.Client()
    client.on_message = on_message

    # Connect to MQTT broker
    client.connect(MQTT_SERVER, MQTT_PORT, 60)

    # Subscribe to the topic
    client.subscribe(MQTT_TOPIC)
    print(f"Subscribed to MQTT topic: {MQTT_TOPIC}")

    # Start the MQTT loop
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("Exiting...")
        client.disconnect()

if __name__ == "__main__":
    main()
