import os
import re
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData, TransportType


# =============================
# LOAD ENVIRONMENT VARIABLES
# =============================
load_dotenv()  # Load .env file

# =============================
# CONFIGURATION
# =============================
EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

# Input folder containing JSON files
DATA_DIR = "/home/memphis/sanju/mqtt_to_cloud/data/Payload"

# Output folder for logs
LOGS_DIR = "/home/memphis/sanju/mqtt_to_cloud/logs"
LAST_SENT_FILE = os.path.join(LOGS_DIR, "last_sent_log.txt")

CHECK_INTERVAL = 5  # seconds
MAX_RETRIES = 3     # retry attempts per file

# =============================
# LOGGING
# =============================
def log_message(message):
    """Prints timestamped log message to console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {message}")

def update_last_sent(filename):
    """Overwrite log file with only the most recent sent file."""
    os.makedirs(LOGS_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LAST_SENT_FILE, "w") as f:
        f.write(f"Sent Timestamp: {timestamp} - File Sent: {filename}\n")
    log_message(f"📝 Updated last sent log: {filename}")

# =============================
# FILE TIMESTAMP SORTING
# =============================
TIMESTAMP_RE = re.compile(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})")

def file_timestamp_key(filename):
    match = TIMESTAMP_RE.search(filename)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y-%m-%d_%H-%M-%S")
        except Exception:
            pass
    path = os.path.join(DATA_DIR, filename)
    return datetime.fromtimestamp(os.path.getmtime(path))

# =============================
# SEND FUNCTION WITH RETRY
# =============================
def send_with_retry(producer, file_path, data):
    """Send JSON data to Event Hub with retry mechanism."""
    data_str = json.dumps(data)
    data_size = len(data_str.encode("utf-8"))

    # Check size before sending
    if data_size > 250000:  # Event Hub limit ~256 KB
        log_message(f"⚠️ File too large to send ({data_size} bytes): {file_path}")
        return False

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            batch = producer.create_batch()
            batch.add(EventData(data_str))
            producer.send_batch(batch)
            log_message(f"✅ Sent: {os.path.basename(file_path)} (Attempt {attempt})")
            return True
        except Exception as e:
            log_message(f"❌ Attempt {attempt} failed for {file_path}: {e}")
            time.sleep(2)  # short delay before retry
    return False

# =============================
# MAIN LOOP
# =============================
def watch_and_send():
    log_message("🚀 EventHub sender started. Watching for new JSON files...")

    # Use WebSocket transport for reliability
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME,
        transport_type=TransportType.AmqpOverWebsocket
    )

    with producer:
        while True:
            try:
                # Get all JSON files in folder
                files = [f for f in os.listdir(DATA_DIR) if f.endswith(".json")]
                if not files:
                    time.sleep(CHECK_INTERVAL)
                    continue

                files.sort(key=file_timestamp_key)
                for filename in files:
                    file_path = os.path.join(DATA_DIR, filename)
                    try:
                        with open(file_path, "r") as f:
                            data = json.load(f)

                        if send_with_retry(producer, file_path, data):
                            update_last_sent(filename)
                            os.remove(file_path)
                            log_message(f"🗑️ Deleted after successful send: {filename}")
                        else:
                            log_message(f"⚠️ Retaining file for retry: {filename}")

                    except Exception as e:
                        log_message(f"❌ Error processing {filename}: {e}")

                time.sleep(CHECK_INTERVAL)

            except KeyboardInterrupt:
                log_message("🛑 Stopped manually by user.")
                break
            except Exception as e:
                log_message(f"⚠️ Unexpected error: {e}")
                time.sleep(CHECK_INTERVAL)

# =============================
# ENTRY POINT
# =============================
if __name__ == "__main__":
    watch_and_send()
