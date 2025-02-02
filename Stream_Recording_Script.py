import os
import time
import json
import uuid
import subprocess
import threading
import logging

# Constants
JSON_CONFIG_FILE = "~/json/config.json"
CAMERA_APP_NAME = "5GCamera*"
SYSTEM_TIMESTAMP_PATH = "./system_timestamp.txt"
SYSTEM_UUID_PATH = "./system_uuid.txt"
CAMERA_APP_RUN_COMMAND = "./5GCamera*"          # Command to start 5GCamera application
CAMERA_APP_LOG_DIR = "~/Camera5gAppLogs"        # Define log directory
PYTHON_SCRIPT_LOG_DIR = "~/PythonScriptLogs"    # Define log directory

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


logger = configure_logger()

def load_config(file_path):
    """Load configuration from JSON file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        return None


def is_network_available(server_ip):
    """Check if the server is reachable via ping"""
    try:
        result = subprocess.run(["ping", "-c", "1", server_ip], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            logger.info(f"Successfully reached server {server_ip}.")
            return True
        else:
            logger.warning(f"Server {server_ip} is unreachable")
            return False
    except Exception as e:
        logger.error(f"Network check failed: {e}")
        return False


def get_running_pid(app_name):
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


def start_application():
    """Start the application in the background and save logs with timestamps"""
    try:
        # Ensure the log directory exists
        if not os.path.exists(LOG_DIR):
            os.makedirs(CAMERA_APP_LOG_DIR)  # Create the log directory if it does not exist

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


def monitor_application():
    """Ensure application is always running"""
    while True:
        pid = get_running_pid(CAMERA_APP_NAME)
        if not pid:
            logger.warning(f"{CAMERA_APP_NAME} is not running!!! Restarting...!!!")
            start_application()
        time.sleep(5)  # Check every 5 seconds


def wait_for_timestamp_file():
    """Wait indefinitely for system_timestamp.txt to appear"""
    while True:
        if os.path.exists(SYSTEM_TIMESTAMP_PATH):
            logger.info(f"Timestamp file found: {SYSTEM_TIMESTAMP_PATH}")
            return True
        logger.warning("Waiting for timestamp file to appear...!!!")
        time.sleep(5)  # Keep checking every 5 seconds


def read_timestamp():
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


def generate_uuid_from_timestamp(timestamp):
    """Generate UUID based on timestamp"""
    new_uuid = uuid.uuid1(node=timestamp)
    logger.info(f"Generated UUID: {new_uuid}")
    return new_uuid


def ensure_uuid_file():
    """Ensure system_uuid.txt exists, and create it if not"""
    if os.path.exists(SYSTEM_UUID_PATH):
        with open(SYSTEM_UUID_PATH, 'r') as file:
            existing_uuid = file.readline().strip()
            if existing_uuid:
                logger.info(f"Existing UUID found: {existing_uuid}")
                return existing_uuid

    # Wait for timestamp file before generating UUID
    wait_for_timestamp_file()

    timestamp = read_timestamp()
    if not timestamp:
        logger.error("Failed to retrieve timestamp. Exiting")
        return None

    new_uuid = generate_uuid_from_timestamp(timestamp)
    with open(SYSTEM_UUID_PATH, 'w') as file:
        file.write(str(new_uuid))
    logger.info(f"New UUID generated and saved: {new_uuid}")
    return str(new_uuid)


def main():
    """Main function to coordinate all tasks."""
    # Load configuration
    config = load_config(CONFIG_FILE)
    if not config:
        logger.error("Configuration could not be loaded. Exiting...!!!")
        return

    server_ip = config.get("signalling_server_ip")
    server_port = config.get("signalling_server_port")

    if not server_ip or not server_port:
        logger.error("Missing server IP or port in config. Exiting...!!!")
        return

    # Check network connectivity
    if not is_network_available(server_ip):
        logger.error(f"Cannot reach server {server_ip}. Exiting...!!!")
        return

    # Check if application is running, start if not
    pid = get_running_pid(APP_NAME)
    if not pid:
        logger.info("Application is not running. Starting it now...!!!")
        start_application()

    # Monitor application in a separate thread
    monitor_thread = threading.Thread(target=monitor_application, daemon=True)
    monitor_thread.start()

    # Ensure UUID file exists or create it
    ensure_uuid_file()


if __name__ == "__main__":
    logger.info("Starting application monitor...")
    main()
