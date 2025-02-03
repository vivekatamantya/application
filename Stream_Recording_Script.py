import os
import time
import shutil
import logging
from datetime import datetime
import json
import websocket
import uuid
import multiprocessing
import threading
import socket

# Constants
FILE_PATH = "./system_uuid.txt"
SOURCE_DIR = "./OutputFiles"
DEST_BASE_DIR = "./StreamRecording"
TIME_STATUS_PATH = "./system_timestamp.txt"
CHECK_INTERVAL = 5
DISK_THRESHOLD = 80
# Generate log file name with timestamp
timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = f"log/file_management_service_{timestamp}.log"
MAX_LOG_FILE_SIZE_MB = 50  # Maximum log file size in MB
CPP_LOG_FILE = "Application_logs_cpp.log" 
COMMAND = f"./5GCamera_v2.0.1 "
# COMMAND = f"./out "

# Function to load config from JSON
def load_config(file_path):
    try:
        with open(file_path, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration file: {e}")
        return None
    
def websocket_connect_and_receive(ws):
    """
    Handle WebSocket communication, including receiving data.
    Returns True if a valid request to send buffered streams is received.
    """
    try:
        message = ws.recv()
        if message:
            request = json.loads(message)
            logger.info(f"Received request: {json.dumps(request, indent=2)}")
            if request.get("type") == "request_buffered_streams":
                logger.info("Valid request received. Preparing to send buffered stream data.")
                return True
    except Exception as e:
        logger.error(f"Error receiving WebSocket message: {e}")
    return False

def is_file_complete(file_path, wait_time=5):
    """Check if the file size remains the same for a certain duration (indicating completion)."""
    try:
        initial_size = os.path.getsize(file_path)
        time.sleep(wait_time)  # Wait for the file to finish writing
        final_size = os.path.getsize(file_path)
        return initial_size == final_size
    except Exception as e:
        logger.error(f"Error checking file completeness for {file_path}: {e}")
        return False


def count_folders_and_files(persistent_folder_list):
    """Count folders and files and collect their metadata."""
    folder_file_dict = {}
    folders = [d for d in os.listdir(DEST_BASE_DIR) if os.path.isdir(os.path.join(DEST_BASE_DIR, d))]
    folder_count = len(folders)

    for folder in folders:
        folder_path = os.path.join(DEST_BASE_DIR, folder)
        file_names = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        folder_file_dict[folder] = []

        for file in file_names:
            file_path = os.path.join(folder_path, file)
            metadata = get_file_metadata(file_path)

            # Only append file if metadata is not None and duration is not "0 seconds"
            if metadata and metadata.get("duration") != "0 seconds":
                folder_file_dict[folder].append({
                    "name": file,
                    "file_size": metadata.get("file_size", "Unknown"),
                    "duration": metadata.get("duration", "0 seconds")
                })

        if folder not in persistent_folder_list:
            persistent_folder_list.append(folder)

    total_files = sum(len(files) for files in folder_file_dict.values())
    logger.info(f"Counted {folder_count} folders and {total_files} files.")
    return folder_file_dict, persistent_folder_list

# Configure logging
def configure_logger():
    """
    Configures and returns a logger instance.
    
    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger("FileManagementLogger")
    logger.setLevel(logging.DEBUG)

    # Prevents adding duplicate handlers if configure_logger is called multiple times
    if not logger.handlers:
        # Create console handler for all levels
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)  # Changed to DEBUG to capture all levels

        # Create file handler with dynamically generated log file name
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setLevel(logging.DEBUG)

        # Formatter for log messages
        formatter = logging.Formatter(
            "[%(levelname)s][%(asctime)s::%(msecs)03d][%(message)s]",
            datefmt="%d-%m-%y %H:%M:%S"
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger

# Initialize logger
logger = configure_logger()

def check_log_file_size():
    
    """Check if the log file size exceeds the maximum limit and trim logs if necessary."""
    if os.path.exists(LOG_FILE):
        file_size_mb = os.path.getsize(LOG_FILE) / (1024 * 1024)
        if file_size_mb > MAX_LOG_FILE_SIZE_MB:
            logger.info(f"Log file size {file_size_mb:.2f}MB exceeds {MAX_LOG_FILE_SIZE_MB}MB. Trimming logs.")
            trim_log_file()

def trim_log_file():
    
    """Trim the log file by removing older entries."""
    with open(LOG_FILE, "r") as file:
        lines = file.readlines()
    half_lines = len(lines) // 2  # Retain only the second half of the file
    with open(LOG_FILE, "w") as file:
        file.writelines(lines[half_lines:])
    logger.info(f"Trimmed log file to retain the last {len(lines) - half_lines} log entries.")

def is_network_available():

    try:
        # Attempt to connect to Google's public DNS server (8.8.8.8) on port 53 (DNS)
        with socket.create_connection(("8.8.8.8", 53), timeout=5):
            logger.debug("Network is available.")
            return True
    except (socket.timeout, socket.error):
        logger.warning("Network is unavailable.")
        return False

def check_storage(ws, camera_id):
    """Check disk usage and delete the oldest folders if usage exceeds the specified threshold."""
    usage = get_disk_usage(SOURCE_DIR)
    logger.info(f"Current disk usage: {usage}%. Threshold: {DISK_THRESHOLD}%.")
    deleted_folders = []  # To keep track of deleted folders

    while usage >= DISK_THRESHOLD:
        oldest_folder = get_oldest_folder(DEST_BASE_DIR)
        if oldest_folder:
            logger.warning(f"Deleting oldest folder: {oldest_folder} to free up disk space.")
            shutil.rmtree(oldest_folder)
            deleted_folders.append(os.path.basename(oldest_folder))  # Keep folder name for WebSocket message
        else:
            logger.warning("No folders found to delete.")
            break
        usage = get_disk_usage(SOURCE_DIR)

    # If any folder was deleted, send the list to WebSocket
    if deleted_folders:
        send_deleted_folders_to_ws(ws, camera_id, deleted_folders)
        
def send_deleted_folders_to_ws(ws, camera_id, deleted_folders):
    """Send a WebSocket message with the list of deleted folders."""
    json_data = {
        "type": "files_deleted",
        "cameraId": camera_id,
        "contents": deleted_folders
    }
    try:
        ws.send(json.dumps(json_data))
        logger.info(f"Deleted Data sent to WebSocket: {json.dumps(json_data, indent=2)}")
    except Exception as e:
        logger.error(f"Failed to send deleted folders data via WebSocket: {e}")

def get_disk_usage(path):
    
    """Calculate disk usage percentage for a path."""
    statvfs = os.statvfs(path)
    total_blocks = statvfs.f_blocks
    free_blocks = statvfs.f_bavail
    used_blocks = total_blocks - free_blocks
    usage_percentage = int(used_blocks / total_blocks * 100)
    logger.debug(f"Disk usage for {path}: {usage_percentage}%.")
    return usage_percentage

def get_oldest_folder(base_dir):
    
    """Find and return the oldest folder."""
    folders = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    if folders:
        oldest_folder = min(folders, key=os.path.getctime)
        logger.info(f"Oldest folder found: {oldest_folder}")
        return oldest_folder
    logger.info("No folders found.")
    return None

def handle_file_processing(ws, camera_id):
    """Monitor for new files and send buffered data when files are found."""
    network_available = is_network_available()  # Initialize network status
    persistent_folder_list = []  # To track folders across checks
    last_created_folder = None  # Track the last folder created for storing files

    while True:
        # Check for any network status change
        new_network_status = is_network_available()
        if new_network_status != network_available:
            logger.info("Network status changed." if new_network_status else "Network unavailable.")
            network_available = new_network_status  # Update network status

        # Check disk usage before moving files
        usage = get_disk_usage(SOURCE_DIR)
        if usage >= DISK_THRESHOLD:
            logger.warning(f"Storage usage is {usage}%, above the threshold. Not moving files.")
        else:
            # Find .mp4 files to move
            mp4_files = [f for f in os.listdir(SOURCE_DIR) if f.endswith(".mp4") and os.path.isfile(os.path.join(SOURCE_DIR, f))]
            if mp4_files:
                # If no folder has been created yet, create one based on timestamp
                if not last_created_folder:
                    first_file = mp4_files[0]
                    timestamp = extract_timestamp(os.path.splitext(first_file)[0]) or datetime.now().strftime("%Y%m%d_%H%M%S")
                    last_created_folder = os.path.join(DEST_BASE_DIR, f"StreamRecording_{timestamp}")
                    os.makedirs(last_created_folder, exist_ok=True)
                    logger.info(f"Created destination folder: {last_created_folder}")

                # Move each detected .mp4 file to the created folder
                for file in mp4_files:
                    src_path = os.path.join(SOURCE_DIR, file)
                    if os.path.getsize(src_path) == 0:
                        logger.warning(f"Skipping {file} as its size is 0 bytes.")
                        continue
                    shutil.move(src_path, os.path.join(last_created_folder, file))
                    logger.info(f"Moved file {file} to {last_created_folder}")

                # Get folder/file metadata and prepare JSON data for WebSocket
                folder_file_list, persistent_folder_list = count_folders_and_files(persistent_folder_list)
                json_data = {
                    "type": "buffered_streams_list",
                    "cameraId": camera_id,  # Camera ID
                    "content": [
                        {
                            "folderName": folder,
                            "content": folder_file_list[folder]
                        } for folder in folder_file_list
                    ]
                }

                # Send JSON data to WebSocket if network is available
                #if network_available:
                #    try:
                #        ws.send(json.dumps(json_data))
                #        logger.info(f"Data sent to WebSocket server. JSON data: {json.dumps(json_data, indent=2)}")
                #    except Exception as e:
                #        logger.error(f"Failed to send data via WebSocket: {e}")

        time.sleep(1)  # Wait for the next check interval

def handle_incoming_requests(ws, camera_id):
    """Handle incoming WebSocket requests."""
    while True:
        message = ws.recv()
        try:
            data = json.loads(message)
            if data.get('type') == 'request_buffered_streams' and data.get('cameraId') == camera_id:
                logger.info(f"Received request for buffered streams from camera: {camera_id}")
                # Scan the destination folder and prepare JSON data
                folder_file_list, _ = count_folders_and_files([])
                json_data = {
                    "type": "buffered_streams_list",
                    "cameraId": camera_id,
                    "content": [
                        {
                            "folderName": folder,
                            "content": folder_file_list[folder]
                        } for folder in folder_file_list
                    ]
                }
                # Send the JSON data back to WebSocket
                try:
                    ws.send(json.dumps(json_data))
                    logger.info(f"Sent buffered stream data for camera {camera_id} to WebSocket.")
                    logger.info(f"Sent buffered stream data json >>>>>> {json.dumps(json_data, indent=2)}")
                except Exception as e:
                    logger.error(f"Failed to send data via WebSocket: {e}")
        except Exception as e:
            logger.error(f"Error processing incoming message: {e}")
        time.sleep(1)

def get_file_metadata(file_path):
    """Retrieve file size in bytes and duration using ffprobe."""
    try:
        # Get file size in bytes
        file_size_bytes = os.path.getsize(file_path)

        # Skip file if size is 0
        if file_size_bytes == 0:
            logger.warning(f"Skipping {file_path} because it has size 0 bytes.")
            return None

        # Get duration using ffprobe via os.popen
        command = f"ffprobe -i {file_path} -show_entries format=duration -v quiet -of csv='p=0'"
        duration_seconds = os.popen(command).read().strip()

        # Check if duration was retrieved successfully
        if duration_seconds:
            duration = f"{float(duration_seconds):.2f} seconds"
        else:
            duration = "0 seconds"
        
        # Skip file if duration is 0 seconds
        if duration == "0 seconds":
            logger.warning(f"Skipping {file_path} because it has a duration of 0 seconds.")
            return None

        metadata = {
            "file_size": f"{file_size_bytes} bytes",
            "duration": duration
        }

        logger.info(f"Retrieved metadata for {file_path}: {metadata}")
        return metadata

    except Exception as e:
        logger.error(f"Error retrieving metadata for {file_path}: {e}")
        return None

def run_cpp_binary():
    
    try:
        # Run the C++ binary in the background using os.fork()
        pid = os.fork()
        command = COMMAND
        # command = COMMAND
        if pid == 0:  # Child process
            # Redirect standard output and error to the log file
            with open(CPP_LOG_FILE, "a") as log_file:
                os.dup2(log_file.fileno(), 1)  # Redirect stdout to log file
                os.dup2(log_file.fileno(), 2)  # Redirect stderr to log file

            # Run the C++ binary with the camera_id as a parameter
            # command = f"./webrtcSendOnly {camera_id}"
            os.system(command)
            logger.info(f"C++ binary {command} is running in the background.")
            os._exit(0)  # Exit child process after running the binary
        else:
            # Parent process continues here
            logger.info(f"C++ binary {command} started in background with PID {pid}.")
    except Exception as e:
        logger.error(f"Failed to run the binary: {e}")

def create_uuid_from_timestamp(timestamp):
    """Creates a UUID based on a given timestamp."""
    try:
        # Generate UUID based on the timestamp using uuid1 (which uses timestamp)
        camera_id = uuid.uuid1(node=timestamp)  # Using node parameter to encode timestamp as node
        logger.info(f"Generated UUID: {camera_id} using timestamp: {timestamp}")
        return camera_id
    except Exception as e:
        logger.error(f"Error creating UUID from timestamp: {e}")
        return None

def write_to_uuid_file(camera_id, timestamp):
    """Write the UUID and timestamp to the uuid.txt file."""
    try:
        with open(FILE_PATH, 'a') as file:
            file.write(f"{camera_id}\n")
        logger.info(f"Appended {camera_id} {timestamp} to uuid.txt")
    except Exception as e:
        logger.error(f"Error writing to uuid.txt: {e}")

def continuously_read_timestamp_and_update_uuid():
    """Continuously reads timestamp from time_status.txt and updates uuid.txt with a new UUID."""
    camera_id = None  # Initialize camera_id variable to return it later
    
    while True:
        try:
            if os.path.exists(TIME_STATUS_PATH):
                with open(TIME_STATUS_PATH, 'r') as file:
                    timestamp_line = file.readline().strip()  # Read the first line from the file
                    if timestamp_line:
                        # Strip out unwanted text and ensure it is a valid integer
                        if 'UTC Timestamp: ' in timestamp_line:
                            timestamp_line = timestamp_line.split('UTC Timestamp: ')[1].strip()
                        
                        try:
                            # Parse the timestamp as an integer
                            timestamp = int(timestamp_line)
                            logger.info(f"Read timestamp: {timestamp} from {TIME_STATUS_PATH}")
                            
                            # Generate UUID using the timestamp
                            camera_id = create_uuid_from_timestamp(timestamp)
                            if camera_id:
                                write_to_uuid_file(camera_id, timestamp)  # Write to uuid.txt
                                return camera_id  # Return the camera_id after creation

                        except ValueError as ve:
                            logger.error(f"Invalid timestamp value: {ve}")

            else:
                logger.warning(f"{TIME_STATUS_PATH} does not exist.")

        except Exception as e:
            logger.error(f"Error reading from {TIME_STATUS_PATH}: {e}")

        # Sleep for a short time before checking again (e.g., 1 second)
        time.sleep(1)

def connect_websocket_with_retries(WS_URL,max_retries=5, delay=2):
    """
    Attempts to establish a WebSocket connection with retries.

    Args:
        max_retries (int): The number of retry attempts.
        delay (int): Delay in seconds between each retry attempt.

    Returns:
        websocket.WebSocket: The WebSocket instance if connection is successful, else None.
    """
    ws = websocket.WebSocket()
    retries = 0

    while retries < max_retries:
        try:
            ws.connect(WS_URL)
            logger.info(f"Connected to WebSocket server at {WS_URL}")
            return ws  # Return the WebSocket instance on successful connection
        except Exception as e:
            retries += 1
            logger.error(f"Failed to connect to WebSocket server (Attempt {retries}/{max_retries}): {e}")
            if retries < max_retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Wait before retrying
            else:
                logger.error("Max retries reached. Could not connect to WebSocket server.")
                return None  # Return None after all retries fail

def main():
    
    # Load server IP and port from JSON file
    config = load_config("/home/root/json/config.json")
    if config:
        # Read server_ip and server_port from the JSON file
        server_ip = config.get("signalling_server_ip")
        server_port = config.get("signalling_server_port")
        check_interval = config.get("check_interval_stream_file")

        # Ensure all required values are present in the config
        if not server_ip or not server_port or check_interval is None:
            logger.error("server_ip, server_port, and/or check_interval missing in the configuration file")
            exit(1)

    WS_URL = f"ws://{server_ip}:{server_port}/"  # Construct WebSocket URL dynamically
    
    # Run the C++ binary in the background
    # run_cpp_binary()
    
    ws = None
    while ws is None:
        if is_network_available():
            ws = connect_websocket_with_retries(WS_URL)
        else:
            logger.warning("Network is unavailable. Retrying in 5 seconds...")
            time.sleep(5)  # Retry after a brief pause
#    camera_id = continuously_read_timestamp_and_update_uuid()
    # camera_id = "76e276ec-6ce1-47c9-8c34-4af35cf8ebf1"   
    camera_id = "6ce1-47c9-8c34-4af35cf8ebf8" 
    # Convert UUID to string for JSON serialization
    camera_id = str(camera_id)
    logger.debug(f"camera_id >>>>> {camera_id}")
    
    json_data = {
        "type": "new_camera_script",
        "cameraId": camera_id
    }
    # Send JSON data to WebSocket if network is available
    if is_network_available() and ws:
        try:
            ws.send(json.dumps(json_data))
            logger.info(f"Data sent to WebSocket server. JSON data: {json.dumps(json_data, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to send data via WebSocket: {e}")
    
    
    
    # Start a thread or process to handle incoming requests
    request_thread = threading.Thread(target=handle_incoming_requests, args=(ws, camera_id))
    request_thread.daemon = True
    request_thread.start()

    # Main loop for file processing
    while True:
        handle_file_processing(ws, camera_id)
        time.sleep(check_interval)  # Pause for defined interval before the next check

def extract_timestamp(basename):
    import re
    match = re.search(r'\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}', basename)
    if match:
        logger.debug(f"Extracted timestamp: {match.group(0)} from {basename}.")
    else:
        logger.debug(f"No timestamp found in {basename}.")
    return match.group(0) if match else None

if __name__ == "__main__":
    logger.info("Starting the file management service.")
    main()