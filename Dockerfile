FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install required libraries
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script into the container
COPY pyais.py .

# Set environment variables
ENV MQTT_SERVER=192.168.1.45
ENV MQTT_PORT=1883
ENV MQTT_TOPIC=ais/data
ENV HA_URI=http://192.168.1.45:8123/api/states/sensor.AISReceived
ENV HA_TOKEN=YOUR_BEARER_TOKEN_HERE

# Run the script
CMD ["python", "pyais.py"]