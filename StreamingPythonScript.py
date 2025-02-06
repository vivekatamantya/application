import os
import time
import json
import uuid
import subprocess
import threading
import logging
import re
import shutil
import traceback
from datetime import datetime
from CustomWebSocketClient import WebSocketClient

# Constants
JSON_CONFIG_FILE = "/home/root/FinalTestBuild/application/json/config.json"
CAMERA_APP_NAME = "5GCamera_v*"
SYSTEM_TIMESTAMP_PATH = "/home/root/FinalTestBuild/application/sys_timestamp.txt"
SYSTEM_UUID_PATH = "/home/root/FinalTestBuild/application/system_uuid.txt"
CAMERA_APP_RUN_COMMAND = "/home/root/FinalTestBuild/application/5GCamera_v*"        # Command to start 5GCamera application
CAMERA_APP_LOG_DIR = "/home/root/FinalTestBuild/application/Camera5gAppLogs"        # Define log directory
PYTHON_SCRIPT_LOG_DIR = "/home/root/FinalTestBuild/application/PythonScriptLogs"    # Define log directory
CAMERA_APP_MAX_LOG_FILES = 5                                                        # Max No of Cpp app to keep 
DEST_BASE_DIR = "/home/root/FinalTestBuild/application/StreamRecordings"
SOURCE_DIR = "/home/root/FinalTestBuild/application/OutputFiles"
CHECK_INTERVAL = 5
DISK_THRESHOLD = 80

# Initialize logger from your function
from logging.handlers import RotatingFileHandler

def configure_logger():
        """Configures and returns a logger instance with log rotation"""
        logger = logging.getLogger("FileManagementLogger")
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            if not os.path.exists(PYTHON_SCRIPT_LOG_DIR):
                os.makedirs(PYTHON_SCRIPT_LOG_DIR)

            # Log file path
            LOG_FILE = os.path.join(PYTHON_SCRIPT_LOG_DIR, "file_management_service.log")  # No timestamp (keeps rotating)

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)

            # Rotating file handler (max 50MB per file, keeps last 3 logs)
            file_handler = RotatingFileHandler(LOG_FILE, maxBytes=50*1024*1024, backupCount=3)
            file_handler.setLevel(logging.DEBUG)

            # Formatter
            formatter = logging.Formatter("[%(levelname)s][%(asctime)s::%(msecs)03d][%(message)s]", datefmt="%d-%m-%y %H:%M:%S")
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)

            logger.addHandler(console_handler)
            logger.addHandler(file_handler)

        return logger
    
# Initialize logger
logger = configure_logger()

class StreamRecoder:
    def __init__(self, config_file = "~/json/config.json", reconnect_attempts=5, reconnect_delay=2):
        self.config_file = config_file
        self.reconnect_attempts = reconnect_attempts
        self.reconnect_delay = reconnect_delay
        #Default values
        self.server_ip = "localhost"
        self.server_port = 8765
        self.camera_id = None
        self.ws_client = None
        self.check_interval = 5
        self.app_uuid = ''
        self.last_created_folder = None  # Track the last folder created for storing files
        self.stop_event = threading.Event()
        self.load_configuration()

    def establish_websocket(self):
        # self.ws_client.connect())  # Runs the function inside sync code
        res=self.ws_client.connect()  # Runs the function inside sync code
        logger.info(f"Websocket connected  at ws://{self.server_ip}:{self.server_port} {res}")
        return self.ws_client.is_connected
        
    def load_configuration(self):
        config = self.load_config()
        if not config:
            logger.error("Configuration could not be loaded. Exiting...!!!")
            return False
        self.server_ip = config.get("signalling_server_ip")
        self.server_port = config.get("signalling_server_port")
        self.check_interval = config.get("check_interval_stream_file")

        if not self.server_ip or not self.server_port or self.check_interval is None:
            logger.error("Missing server IP or port in config. Exiting...!!!")
            return False
        
        self.ws_client = WebSocketClient(self.server_ip, self.server_port)

    def load_config(self):
        """Load configuration from JSON file"""
        try:
            config_file_path = os.path.expanduser(self.config_file)
            with open(config_file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config file: {e}")
            return None

    def is_network_available(self):
        """Check if the server is reachable via ping"""
        try:
            result = subprocess.run(["ping", "-c", "1", "8.8.8.8"], stdout=subprocess.PIPE, stderr=subprocess.PIPE,timeout=5)
            if result.returncode == 0:
                logger.info(f"Successfully reached 8.8.8.8.")
                return True
            else:
                logger.warning(f"Server is unreachable")
                return False
        except Exception as e:
            logger.error(f"Network check failed: {e}")
            return False

    def wait_for_network(self,max_retry: int = 10, retry_delay: int = 5) -> bool:
        """
        Wait for the network to become available by pinging the server IP.
        
        :param server_ip: The IP address of the server to ping.
        :param max_retry: Maximum number of retry attempts.
        :param retry_delay: Delay between retry attempts in seconds.
        :return: True if the network becomes available, False otherwise.
        """
        attempt = 0
        while attempt < max_retry:
            attempt += 1
            logger.info(f"Attempt {attempt}/{max_retry}: Checking network availability...")
            if self.is_network_available():
                logger.info(f"Network is available after {attempt} attempts.")
                return True
            else:
                logger.warning(f"Network is unavailable. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
        
        logger.error(f"Network is unavailable after {max_retry} attempts.")
        return False

    def get_running_pid(self,app_name):
        """Check if application is running and return its PID"""
        try:
            result = subprocess.run(['pgrep', '-f', app_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.stdout.strip():
                pid = int(result.stdout.strip().split("\n")[0])  # Return first PID found
                logger.info(f"Application {app_name} is running with PID {pid}.")
                return pid
        except Exception as e:
            logger.error(f"Error checking application status: {e}")
        return None

    def clean_old_logs(self, log_dir, max_logs=5):
        """Deletes old log files while keeping the most recent ones."""
        try:
            # Ensure the log directory exists
            if not os.path.exists(CAMERA_APP_LOG_DIR):
                os.makedirs(CAMERA_APP_LOG_DIR)  # Create the log directory if it does not exist
                return
            
            log_files = [f for f in os.listdir(log_dir) if f.endswith(".log")]
            
            if len(log_files) > max_logs:
                log_files_with_time = []
                
                for f in log_files:
                    file_path = os.path.join(log_dir, f)
                    try:
                        creation_time = os.stat(file_path).st_mtime  # Use modification time instead of ctime for better reliability
                        log_files_with_time.append((file_path, creation_time))
                    except OSError as e:
                        logger.warning(f"Skipping file {file_path} due to error: {e}")

                # Sort by modification time (oldest first)
                log_files_with_time.sort(key=lambda x: x[1])
                
                # Keep only the latest `max_logs` files, delete the rest
                files_to_delete = log_files_with_time[:-max_logs]
                
                for file_path, _ in files_to_delete:
                    try:
                        os.remove(file_path)
                        logger.info(f"Deleted old log file: {file_path}")
                    except OSError as e:
                        logger.error(f"Failed to delete {file_path}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during log cleanup: {e}")
        
    def start_application(self):
        """Start the application in the background and save logs with timestamps"""
        try:
            # Generate log file name with timestamp
            timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
            log_file_path = os.path.join(CAMERA_APP_LOG_DIR, f"application_{timestamp}.log")

            # Open the log file and redirect stdout & stderr
            with open(log_file_path, "a") as log_file:
                process = subprocess.Popen(CAMERA_APP_RUN_COMMAND, shell=True, stdout=log_file, stderr=log_file)

            logger.info(f"Started application {CAMERA_APP_RUN_COMMAND} with PID {process.pid}, logs: {log_file_path}")
            return process.pid

        except Exception as e:
            logger.error(f"Failed to start application: {e}")
            return None

    def monitor_cam_disk_application(self):
        """Ensure application is always running"""
        while not self.stop_event.is_set():
            try:
                pid = self.get_running_pid(CAMERA_APP_NAME)
                if not pid:
                    logger.warning(f"{CAMERA_APP_NAME} is not running!!! Restarting...!!!")
                    self.start_application()
                #Handle File log & storage
                self.handle_file_processing()
                time.sleep(5)  # Check every 5 seconds
            except Exception as e:
                logger.error(f"Error monitoring the app: {e}")


    def wait_for_timestamp_file(self):
        """Wait indefinitely for system_timestamp.txt to appear"""
        while True:
            if os.path.exists(SYSTEM_TIMESTAMP_PATH):
                logger.info(f"Timestamp file found: {SYSTEM_TIMESTAMP_PATH}")
                return True
            logger.warning("Waiting for timestamp file to appear...!!!")
            time.sleep(5)  # Keep checking every 5 seconds


    def read_timestamp(self):
        """Read the timestamp from system_timestamp.txt or fallback to current system time if corrupted"""
        try:
            if os.path.exists(SYSTEM_TIMESTAMP_PATH):
                with open(SYSTEM_TIMESTAMP_PATH, 'r') as file:
                    timestamp = file.readline().strip()

                    # Check if timestamp is a valid integer
                    if timestamp.isdigit():
                        logger.info(f"Read valid timestamp: {timestamp}")
                        return int(timestamp)
                    else:
                        logger.warning("Invalid timestamp format in system_timestamp.txt. Using system time")

            else:
                logger.warning(f"{SYSTEM_TIMESTAMP_PATH} does not exist. Using system time")

        except Exception as e:
            logger.error(f"Error reading timestamp: {e}")

        # If the file is missing or corrupt, return current system time as fallback
        fallback_timestamp = int(time.time())
        logger.info(f"Using system timestamp as fallback: {fallback_timestamp}")
        return fallback_timestamp

    def generate_uuid_from_timestamp(self,timestamp):
        """Generate UUID based on timestamp"""
        new_uuid = uuid.uuid1(node=timestamp)
        logger.info(f"Generated UUID: {new_uuid}")
        return new_uuid


    def ensure_uuid_file(self):
        """Ensure system_uuid.txt exists, and create it if not"""
        if os.path.exists(SYSTEM_UUID_PATH):
            with open(SYSTEM_UUID_PATH, 'r') as file:
                existing_uuid = file.readline().strip()
                if existing_uuid:
                    logger.info(f"Existing UUID found: {existing_uuid}")
                    self.app_uuid = existing_uuid
                    return existing_uuid

        # Wait for timestamp file before generating UUID
        self.wait_for_timestamp_file()

        timestamp = self.read_timestamp()
        if not timestamp:
            logger.error("Failed to retrieve timestamp. Exiting")
            return None

        new_uuid = self.generate_uuid_from_timestamp(timestamp)
        with open(SYSTEM_UUID_PATH, 'w') as file:
            file.write(str(new_uuid))
        logger.info(f"New UUID generated and saved: {new_uuid}")
        self.app_uuid = str(new_uuid)
        return self.app_uuid
    
    def register_camera_script(self,camera_id):
        self.camera_id = camera_id
        json_data = {
            "type": "new_camera_script",
            "cameraId": self.camera_id
        }
        # Send JSON data to WebSocket if network is available
        try:
            self.ws_client.send(json.dumps(json_data),True)
            logger.info(f"Data sent to WebSocket server. JSON data: {json.dumps(json_data, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to send data via WebSocket: {e}")

    def handle_incoming_requests(self):
        while True:
            try:
                message = self.ws_client.receive()
                logger.info(message)
                data = json.loads(message)
                if data.get('type') == 'request_buffered_streams' and data.get('cameraId') == self.camera_id:
                    logger.info(f"Received request for buffered streams from camera: {self.camera_id}")
                    # Scan the destination folder and prepare JSON data
                    folder_file_list, _ = self.count_folders_and_files([])
                    json_data = {
                        "type": "buffered_streams_list",
                        "cameraId": self.camera_id,
                        "content": [
                            {
                                "folderName": folder,
                                "content": folder_file_list[folder]
                            } for folder in folder_file_list
                        ]
                    }
                    # Send the JSON data back to WebSocket
                    try:
                        self.ws_client.send(json.dumps(json_data))
                        logger.info(f"Sent buffered stream data for camera {self.camera_id} to WebSocket.")
                        logger.info(f"Sent buffered stream data json >>>>>> {json.dumps(json_data, indent=2)}")
                    except Exception as e:
                        logger.error(f"Failed to send data via WebSocket: {e}")
            except KeyboardInterrupt:
                logger.info("Exiting...")
                break
            
            except Exception as e:
                logger.error(f"Error processing incoming message: {e} :: {message}")
            time.sleep(1)
            
    def get_file_metadata(self, file_path):
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

    def count_folders_and_files(self, persistent_folder_list):
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
                metadata = self.get_file_metadata(file_path)

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
    
    def kill_app(self, app_name):
        """Kill all processes matching the given application name."""
        try:
            result = subprocess.run(['pgrep', '-f', app_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            pids = result.stdout.strip().split("\n")
            if pids and pids[0]:  # Ensure there's at least one PID
                for pid in pids:
                    subprocess.run(['kill', '-9', pid], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    logger.info(f"Killed process {pid} for application {app_name}.")
                return True
            else:
                logger.info(f"No running processes found for {app_name}.")
        except Exception as e:
            logger.error(f"Error while killing processes: {e}")

        return False

    def get_disk_usage(self, path):
        """Calculate disk usage percentage for a path."""
        try:
            if not os.path.exists(path):
                logger.error(f"Path does not exist: {path}")
                return 0  # Return 0% usage if the path is invalid
            
            statvfs = os.statvfs(path)
            total_blocks = statvfs.f_blocks
            available_blocks = statvfs.f_bavail  # Use f_bavail (available to non-root users)
            
            if total_blocks == 0:  # Prevent division by zero
                logger.warning(f"Total disk blocks reported as zero for path: {path}")
                return 0

            used_blocks = total_blocks - available_blocks
            usage_percentage = int((used_blocks / total_blocks) * 100)

            logger.debug(f"Disk usage for {path}: {usage_percentage}%")
            return usage_percentage

        except FileNotFoundError:
            logger.error(f"Invalid path for disk usage check: {path}")
            return 0
        except Exception as e:
            logger.error(f"Unexpected error checking disk usage for {path}: {e}")
            return 0

    def extract_timestamp(self,basename):
        """Extract timestamp from the file."""
        match = re.search(r'\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}', basename)
        if match:
            logger.debug(f"Extracted timestamp: {match.group(0)} from {basename}.")
        else:
            logger.debug(f"No timestamp found in {basename}.")
        return match.group(0) if match else None

    def handle_file_processing(self):
        """Monitor for new files and send buffered data when files are found."""
        
        try:
            # Ensure source directory exists
            if not os.path.exists(SOURCE_DIR):
                logger.error(f"Source directory {SOURCE_DIR} does not exist.")
                return
            if not os.path.exists(DEST_BASE_DIR):
                logger.error(f"Destination base directory {DEST_BASE_DIR} does not exist.")
                return
            # Check disk usage before moving files
            usage = self.get_disk_usage(SOURCE_DIR)
            if usage >= DISK_THRESHOLD:
                logger.warning(f"Storage usage is {usage}%, above the threshold. Not moving files.")
                return
            # Find .mp4 files to move
            mp4_files = [f for f in os.listdir(SOURCE_DIR) if f.endswith(".mp4") and os.path.isfile(os.path.join(SOURCE_DIR, f))]
            if not mp4_files:
                logger.info("No new .mp4 files found.")
                return
            # If no folder has been created yet, create one based on timestamp
            if not self.last_created_folder or not os.path.exists(self.last_created_folder):
                first_file = mp4_files[0]
                timestamp = self.extract_timestamp(os.path.splitext(first_file)[0]) or datetime.now().strftime("%Y%m%d_%H%M%S")
                self.last_created_folder = os.path.join(DEST_BASE_DIR, f"StreamRecording_{timestamp}")
                os.makedirs(self.last_created_folder, exist_ok=True)
                logger.info(f"Created destination folder: {self.last_created_folder}")

            # Move each detected .mp4 file to the created folder
            for file in mp4_files:
                src_path = os.path.join(SOURCE_DIR, file)
                if os.path.getsize(src_path) == 0:
                    logger.warning(f"Skipping {file} as its size is 0 bytes.")
                    continue

                try:
                    shutil.move(src_path, os.path.join(self.last_created_folder, file))
                    logger.info(f"Moved file {file} to {self.last_created_folder}")
                except Exception as move_err:
                    logger.error(f"Failed to move {file}: {move_err}\n{traceback.format_exc()}")

        except Exception as err:
            logger.error(f"Error handling file processing: {err}\n{traceback.format_exc()}")

    def close_connection(self):
        """Close the WebSocket connection."""
        if self.ws_client:
            self.ws_client.disconnect()
        
    def cleanup(self):
        """Clean up resources by closing connections and stopping the camera app."""
        self.close_connection()
        self.kill_app(CAMERA_APP_NAME)
        
def main():
    try:
        streamer = StreamRecoder(JSON_CONFIG_FILE)
        
        streamer.clean_old_logs(CAMERA_APP_LOG_DIR,CAMERA_APP_MAX_LOG_FILES)
        #Check Camera APP Running or not                                                                
        logger.info("Checking Camera Application..")                                                             
        pid = streamer.get_running_pid(CAMERA_APP_NAME)                                       
        if not pid:                                                                           
            logger.info("Application is not running. Starting it now...!!!")                  
            streamer.start_application()  
        
        logger.info("Starting Camera monitor thread..")
        # Monitor application in a separate thread
        monitor_thread = threading.Thread(target=streamer.monitor_cam_disk_application, daemon=True)
        monitor_thread.start()

        #Check Network
        res=streamer.wait_for_network()
        if not res:
            logger.error(f"Cannot reach network. Exiting...!!!")
            return
        #Connect Websocket
        logger.info("Connecting Websocket")
        res = streamer.establish_websocket()
        if not res:
            logger.error(f"Cannot esatblish websocket connection. Exiting...!!!")
            return
        
        # Ensure UUID file exists or create it
        uuid_app = streamer.ensure_uuid_file()
        logger.info(f"UUID Data: {uuid_app}")
        
        #handle_websocket
        streamer.register_camera_script(uuid_app)
        streamer.handle_incoming_requests()
        
        streamer.stop_event.set()  # Signal the thread to stop
        monitor_thread.join()  # Wait for the thread to exit

    except Exception as err:
        logger.error(f"Error :: main :: {err}")
    finally:
        streamer.cleanup()

if __name__ == "__main__":
    logger.info("Starting application monitor...")
    main()
