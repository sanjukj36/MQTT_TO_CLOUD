import paho.mqtt.client as mqtt
import json
import os
from datetime import datetime

# MQTT Configuration
BROKER = "localhost"
PORT = 1883
TOPIC = "ndc/min"

# Output folder
OUTPUT_DIR = "/home/memphis/sanju/mqtt_to_cloud/data/Payload"

# Ensure directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker!")
        client.subscribe(TOPIC)
    else:
        print(f"❌ Failed to connect, return code {rc}")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        print("📥 Received:", payload)

        # Format filename using timestamp (use current UTC if not in payload)
        # timestamp = payload.get("time") or datetime.utcnow().isoformat()
        # safe_timestamp = timestamp.replace(":", "-").replace("T", "_").replace("Z", "")
        # print(timestamp)
        # filename = f"ndc_tel_{safe_timestamp}.json"
        # timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        print("timestamp",timestamp)


        filename = f"MDC_Tele_{timestamp}.json"



        filepath = os.path.join(OUTPUT_DIR, filename)

        # Save payload to new JSON file
        with open(filepath, "w") as f:
            json.dump(payload, f, indent=4)

        print(f"✅ Saved: {filepath}\n")

    except Exception as e:
        print("⚠️ Error processing message:", e)


# MQTT Client setup
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect and start loop
try:
    client.connect(BROKER, PORT, 60)
    print(f"📡 Listening on topic '{TOPIC}'...\n")
    client.loop_forever()
except KeyboardInterrupt:
    print("\n🛑 Stopped by user.")
except Exception as e:
    print("❌ Connection failed:", e)
