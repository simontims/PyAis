import os
import json
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import time
import requests
import logging
import pytz
import sys
import glob
sys.stdout.reconfigure(line_buffering=True)

# Rate limit management
LAST_AISHUB_CALL = 0
AISHUB_RATE_LIMIT = 60  # 1 call every 60 seconds



# Configure logging
logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

# Get timezone from environment variable
timezone_env = os.getenv("TZ")
if not timezone_env:
    raise ValueError("The 'TZ' environment variable is not set. Please configure it in the compose file.")

try:
    LOCAL_TIMEZONE = pytz.timezone(timezone_env)
except pytz.UnknownTimeZoneError:
    raise ValueError(f"The timezone '{timezone_env}' is not recognized. Please check the 'TZ' value.")

def get_local_time():
    """Get the current local time in the configured timezone."""
    return datetime.now(LOCAL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

# Environment variables
MQTT_SERVER = os.getenv("MQTT_SERVER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))  # Default to 1883 as it's a standard MQTT port
HA_SERVER_URL = os.getenv("HA_SERVER_URL")
HA_TOKEN = os.getenv("HA_TOKEN")
AISHUB_USERNAME = os.getenv("AISHUB_USERNAME")
MQTT_TOPICS = os.getenv("MQTT_TOPICS").split(",")  # Comma-separated list of topics
DATA_FILE_PATH = "/data/mmsi_data.json"  # Hardcoded path for persistent storage
IGNORE_TYPES = os.getenv("IGNORE_TYPES", "").split(",")  # Comma-separated list of message types to ignore

# Parse MMSI_TYPE_THROTTLE from the environment
MMSI_TYPE_THROTTLE = json.loads(os.getenv("MMSI_TYPE_THROTTLE", "[]"))

# Track last processed time for throttling
throttle_tracker = {}

# Logging received JSON messages
LOG_RECEIVED_JSON = os.getenv("LOG_RECEIVED_JSON", "false").lower() == "true"
LOG_RECEIVED_JSON_PATH = "/data/inputLog"  # Hardcoded path

# Logging JSON retention period in minutes
LOG_RECEIVED_MINUTES = int(os.getenv("LOG_RECEIVED_MINUTES", 5))  # Default to 5 minutes

# Ensure the log directory exists
if LOG_RECEIVED_JSON:
    os.makedirs(LOG_RECEIVED_JSON_PATH, exist_ok=True)

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
mmsi_name_lookup = {}

def fetch_name_from_aishub(mmsi):
    """Fetch vessel name from AISHub using MMSI."""
    global LAST_AISHUB_CALL

    # Respect the global rate limit
    time_since_last_call = time.time() - LAST_AISHUB_CALL
    if time_since_last_call < AISHUB_RATE_LIMIT:
        logger.info(f"AISHub skipped, next call allowed in {AISHUB_RATE_LIMIT - time_since_last_call:.2f} seconds")
        return None

    # Construct API URL
    uri = f"https://data.aishub.net/ws.php?username={AISHUB_USERNAME}&format=1&output=json&compress=0&mmsi={mmsi}"
    logger.info(f"Making AISHub API request to: {uri}")

    try:
        response = requests.get(uri, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Update last call timestamp
        LAST_AISHUB_CALL = time.time()

        # Check for errors in the response
        if isinstance(data, list) and "ERROR" in data[0] and data[0]["ERROR"]:
            logger.warning(f"AISHub error: {data[0].get('ERROR_MESSAGE', 'Unknown error')}")
            return None

        # Extract name from the response
        if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list) and len(data[1]) > 0:
            name = data[1][0].get("NAME")
            if name:
                logger.info(f"Fetched '{name}' from AISHub.")
                return name

        logger.warning(f"No valid name found for MMSI {mmsi} in AISHub response")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching name from AISHub for MMSI {mmsi}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error processing AISHub response for MMSI {mmsi}: {e}")
    return None

def load_mmsi_data():
    """Load MMSI data and MMSI-to-name lookup from a JSON file."""
    global mmsi_data, mmsi_name_lookup
    if os.path.exists(DATA_FILE_PATH):
        try:
            with open(DATA_FILE_PATH, 'r') as file:
                raw_data = json.load(file)
                mmsi_data = {
                    topic: [(entry["mmsi"], datetime.fromisoformat(entry["timestamp"]))
                            for entry in entries]
                    for topic, entries in raw_data.get("mmsi_data", {}).items()
                }
                mmsi_name_lookup = raw_data.get("mmsi_name_lookup", {})
                logger.info(f"Loaded MMSI data and name lookup from {DATA_FILE_PATH}")
        except Exception as e:
            logger.error(f"Failed to load MMSI data: {e}")
    else:
        logger.info(f"No data file found at {DATA_FILE_PATH}, starting fresh.")
        mmsi_data = {}
        mmsi_name_lookup = {}

def save_mmsi_data():
    """Save MMSI data and MMSI-to-name lookup to a JSON file."""
    try:
        raw_data = {
            "mmsi_data": {
                topic: [{"mmsi": m, "timestamp": t.isoformat()} for m, t in entries]
                for topic, entries in mmsi_data.items()
            },
            "mmsi_name_lookup": mmsi_name_lookup  # Persist the name mapping
        }
        with open(DATA_FILE_PATH, 'w') as file:
            json.dump(raw_data, file)
        # logger.info(f"Saved {DATA_FILE_PATH}")
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
        logger.info(f"Posted to: {sensor_url}")
        logger.info(f"Payload: {json.dumps(payload)}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to post: {e}")

def on_message(client, userdata, message):
    """Handle incoming MQTT messages."""
    global mmsi_data, mmsi_name_lookup

    # Perform cleanup before processing the message
    cleanup_old_json_logs()

    topic = message.topic
    sensor_url = TOPIC_TO_SENSOR.get(topic)
    if not sensor_url:
        logger.warning(f"Received message on unconfigured topic: {topic}")
        return

    try:
        # Parse the incoming message
        data = json.loads(message.payload.decode("utf-8"))

        if LOG_RECEIVED_JSON:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = os.path.join(LOG_RECEIVED_JSON_PATH, f"{timestamp}_{topic.replace('/', '_')}.json")
                with open(filename, "w") as json_file:
                    json.dump(data, json_file, indent=2)
                # logger.info(f"Logged JSON to: {filename}")
            except Exception as log_error:
                logger.error(f"Failed to log received JSON: {log_error}")

        mmsi = data.get("mmsi")
        shipname = data.get("shipname")
        name = data.get("name")
        type = data.get("type")
        timestamp = datetime.now()

        # Convert IGNORE_TYPES to integers and compare
        ignore_types = set(map(int, IGNORE_TYPES))  # Convert to a set of integers
        if type in ignore_types:
            logger.info(f"Ignoring type {type} on {topic}")
            return
        
        # Apply throttling
        if not should_process_message(mmsi, type):
            logger.info(f"Throttled {mmsi} type {type}")
            return

        logger.info(f"Received type {type} on {topic}")

        if not mmsi:
            logger.warning("Message missing MMSI, skipping.")
            return

        # Handle name and shipname prioritization
        if shipname and name:
            name = shipname
            logger.info(f"Got name and shipname, using {name}")
        elif not name and shipname:
            name = shipname
            logger.info(f"Got shipname: {name}")
        elif not name:
            logger.info(f"No name or shipname")
            name = mmsi_name_lookup.get(str(mmsi), None)

            if name:
                logger.info(f"Found '{name}' in cache.")
            else:
                logger.info(f"Name not in cache, attempting fetch from AISHub")
                fetched_name = fetch_name_from_aishub(mmsi)

                if fetched_name:
                    # Cache only valid names
                    mmsi_name_lookup[str(mmsi)] = fetched_name
                    save_mmsi_data()  # Save updated cache
                    name = fetched_name
                else:
                    logger.info(f"No valid name from AISHub for {mmsi}, using 'Unknown'")
                    name = "Unknown"
        else:
            logger.info(f"Got name: {name}")

        # Add to cache only if the name is new and valid
        if name != "Unknown" and str(mmsi) not in mmsi_name_lookup:
            mmsi_name_lookup[str(mmsi)] = name
            save_mmsi_data()
            logger.info(f"Added '{name}' to cache for MMSI {mmsi}.")

        # Initialize tracking for this topic if not already present
        if topic not in mmsi_data:
            mmsi_data[topic] = []

        # Add the MMSI with a timestamp to the tracking list
        mmsi_data[topic].append((mmsi, timestamp))

        # Remove MMSIs older than 60 minutes
        cutoff = datetime.now() - timedelta(minutes=60)
        new_topic_data = [(m, t) for m, t in mmsi_data[topic] if t > cutoff]

        # Save MMSI data only if changes occurred
        if len(new_topic_data) != len(mmsi_data[topic]):
            mmsi_data[topic] = new_topic_data
            save_mmsi_data()
            logger.info("Updated cache")

        # Calculate unique MMSIs
        unique_mmsis = {m for m, t in mmsi_data[topic]}
        vessel_count = len(unique_mmsis)

        # Post to Home Assistant
        post_to_home_assistant(sensor_url, name, mmsi, vessel_count)

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def cleanup_old_json_logs():
    """Remove JSON files older than the retention period."""
    if not LOG_RECEIVED_JSON:
        return

    try:
        cutoff_time = datetime.now() - timedelta(minutes=LOG_RECEIVED_MINUTES)
        for file_path in glob.glob(f"{LOG_RECEIVED_JSON_PATH}/*.json"):
            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
            if file_mtime < cutoff_time:
                os.remove(file_path)
                logger.info(f"Deleted old JSON log: {file_path}")
    except Exception as e:
        logger.error(f"Error during log cleanup: {e}")

def should_process_message(mmsi, msg_type):
    """
    Determine if a message should be processed based on throttling rules.
    Returns True if the message should be processed; otherwise, False.
    """
    for rule in MMSI_TYPE_THROTTLE:
        if rule["mmsi"] == mmsi and rule["type"] == msg_type:
            throttle_key = f"{mmsi}_{msg_type}"
            throttle_interval = rule.get("throttleInterval", 30)  # Default to 30 seconds
            current_time = time.time()

            # Check if enough time has passed since the last processed message
            last_processed_time = throttle_tracker.get(throttle_key, 0)
            if current_time - last_processed_time < throttle_interval:
                return False  # Skip this message
            throttle_tracker[throttle_key] = current_time
    return True

def main():
    """Main function to set up MQTT client and listen for messages."""
    logger.info("Script started")
    logger.info(f"Configured topics: {MQTT_TOPICS}")
    logger.info(f"IGNORE_TYPES: {IGNORE_TYPES}")
    logger.info(f"AISHUB_USERNAME: {AISHUB_USERNAME}")

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
