import os
import time
import shutil
import logging
import json
import websocket
import uuid
import socket
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Constants
JSON_CONFIG_PATH = "~/json/config.json"
SYSTEM_TIMESTAMP_FILE_PATH = "./system_timestamp.txt"
SYSTEM_UUID_FILE_PATH = "./system_uuid.txt"
CAMERA_APP_BUFFER_FILE_SRC_DIR = "./OutputFiles/"
BUFFER_STREAM_FILES_DIR = "./StreamRecording/"

DISK_THRESHOLD = 80
APP_LOG_FILE_NAME = "app.log"
COMMAND = f"./out"

# Configure logging
def configure_logger():
    logger = logging.getLogger("FileManagementLogger")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler(APP_LOG_FILE_NAME)
        file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter("[%(levelname)s][%(asctime)s] %(message)s", datefmt="%d-%m-%y %H:%M:%S")
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger

logger = configure_logger()

def create_uuid_from_timestamp(timestamp):
    """Generate a UUID based only on the given timestamp, if system_uuid.txt exists, read UUID from it, otherwise, generate a new UUID and save it to the file"""
    # If UUID file exists, read from it
    if os.path.exists(SYSTEM_UUID_FILE_PATH):
        try:
            with open(SYSTEM_UUID_FILE_PATH, "r") as file:
                existing_uuid = file.readline().strip()
                if existing_uuid:  # Ensure file is not empty
                    logger.info(f"Using existing UUID from {SYSTEM_UUID_FILE_PATH}: {existing_uuid}")
                    return existing_uuid
        except Exception as e:
            logger.error(f"Error reading UUID file: {e}. Generating new UUID.")

    # Generate new UUID if file does not exist or is empty
    namespace = uuid.NAMESPACE_DNS
    camera_id = uuid.uuid5(namespace, str(timestamp))

    # Write the new UUID to system_uuid.txt
    try:
        with open(SYSTEM_UUID_FILE_PATH, "w") as file:
            file.write(str(camera_id))
        logger.info(f"Generated new UUID and saved to {SYSTEM_UUID_FILE_PATH}: {camera_id}")
    except Exception as e:
        logger.error(f"Error writing UUID file: {e}")

    return str(camera_id)

def is_network_available():
    """Check if network is available by connecting to Google's DNS"""
    try:
        with socket.create_connection(("8.8.8.8", 53), timeout=5):
            return True
    except (socket.timeout, socket.error):
        return False

def get_timestamp():
    """Read timestamp from system_timestamp.txt if it exists. If not, wait until the file appears and then read"""
    while not os.path.exists(SYSTEM_TIMESTAMP_FILE_PATH):  
        logger.info(f"Waiting for {SYSTEM_TIMESTAMP_FILE_PATH} to appear...!!!")
        time.sleep(1)  # Check every second

    try:
        with open(SYSTEM_TIMESTAMP_FILE_PATH, "r") as file:
            timestamp_str = file.readline().strip()
            if timestamp_str.isdigit():  # Ensure it's a valid number
                return int(timestamp_str)
            else:
                logger.info(f"Invalid timestamp in {SYSTEM_TIMESTAMP_FILE_PATH}. Using current time instead")
                return int(time.time())  # Fallback to current UNIX timestamp if invalid
    except Exception as e:
        logger.error(f"Error reading timestamp file: {e}. Using current time instead")
        return int(time.time())
        
def connect_websocket_with_retries(WS_URL, max_retries=5, delay=2):
    """Attempt to establish a WebSocket connection with retries"""
    ws = websocket.WebSocket()
    retries = 0
    while retries < max_retries:
        try:
            ws.connect(WS_URL)
            logger.info(f"Connected to WebSocket server at {WS_URL}")
            return ws
        except Exception as e:
            retries += 1
            logger.error(f"WebSocket connection failed (Attempt {retries}/{max_retries}): {e}")
            if retries < max_retries:
                time.sleep(delay)
            else:
                logger.error("Max retries reached. Could not connect to WebSocket")
                return None

def get_disk_usage(path):
    """Calculate disk usage percentage correctly"""
    statvfs = os.statvfs(path)
    total_space = statvfs.f_blocks * statvfs.f_bsize
    free_space = statvfs.f_bavail * statvfs.f_bsize
    used_space = total_space - free_space
    usage_percentage = int((used_space / total_space) * 100)
    return usage_percentage

def get_oldest_folder(base_dir):
    """Find and return the oldest folder"""
    folders = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    if folders:
        return min(folders, key=os.path.getctime)
    return None

def run_cpp_binary():
    """Run the C++ binary in the background with proper logging"""
    try:
        process = subprocess.Popen(COMMAND, stdout=open(APP_LOG_FILE_NAME, "a"), stderr=subprocess.STDOUT, shell=True)
        logger.info(f"C++ binary '{COMMAND}' started with PID {process.pid}")
    except Exception as e:
        logger.error(f"Failed to run the binary: {e}")

def send_ws_message(ws, data):
    """Send a WebSocket message and handle errors"""
    try:
        ws.send(json.dumps(data))
        logger.info(f"Data sent to WebSocket: {json.dumps(data, indent=2)}")
    except Exception as e:
        logger.error(f"Failed to send WebSocket message: {e}")

def handle_incoming_requests(ws, camera_id):
    """Handle incoming WebSocket requests"""
    while True:
        try:
            ws.settimeout(10)
            message = ws.recv()
            ws.settimeout(None)

            if message:
                data = json.loads(message)
                if data.get('type') == 'request_buffered_streams' and data.get('cameraId') == camera_id:
                    logger.info(f"Received request for buffered streams from camera: {camera_id}")
                    send_ws_message(ws, {"type": "buffered_streams_list", "cameraId": camera_id})
        except websocket.WebSocketTimeoutException:
            pass
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}")
            ws = connect_websocket_with_retries(WS_URL)

class FileHandler(FileSystemEventHandler):
    """Event-driven file monitoring using watchdog"""
    def __init__(self, ws, camera_id):
        self.ws = ws
        self.camera_id = camera_id

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".mp4"):
            logger.info(f"New MP4 file detected: {event.src_path}")
            send_ws_message(self.ws, {"type": "new_file", "cameraId": self.camera_id, "file": event.src_path})

def start_file_observer(ws, camera_id):
    """Start the watchdog file observer"""
    event_handler = FileHandler(ws, camera_id)
    observer = Observer()
    observer.schedule(event_handler, CAMERA_APP_BUFFER_FILE_SRC_DIR, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

def main():
    """Main function to initialize services and WebSocket communication"""
    config = None
    if os.path.exists(JSON_CONFIG_PATH):
        with open(JSON_CONFIG_PATH, 'r') as f:
            config = json.load(f)

    if not config:
        logger.error("Configuration file not found or invalid")
        return

    server_ip = config.get("signalling_server_ip")
    server_port = config.get("signalling_server_port")

    if not server_ip or not server_port:
        logger.error("Missing signalling server ip or port in config file")
        return

    WS_URL = f"ws://{server_ip}:{server_port}/"
    run_cpp_binary()

    if is_network_available():
        ws = connect_websocket_with_retries(WS_URL)
        if ws is None:
            logger.error("Could not establish WebSocket connection.")
            return

    timestamp = get_timestamp()
    camera_id = create_uuid_from_timestamp(timestamp)
    logger.info(f"Generated Camera ID (UUID): {camera_id}")

    send_ws_message(ws, {"type": "new_camera_script", "cameraId": camera_id})

    request_thread = threading.Thread(target=handle_incoming_requests, args=(ws, camera_id))
    request_thread.daemon = True
    request_thread.start()

    start_file_observer(ws, camera_id)

if __name__ == "__main__":
    logger.info("Starting the file management service")
    main()

