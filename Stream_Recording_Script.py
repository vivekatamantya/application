import os
import time
import json
import uuid
import subprocess
import threading
import logging
import glob

# Constants
JSON_CONFIG_FILE = "/home/root/json/config.json"
SYSTEM_TIMESTAMP_PATH = "./system_timestamp.txt"
SYSTEM_UUID_PATH = "./system_uuid.txt"
CAMERA_APP_LOG_DIR = "/home/root/Camera5gAppLogs"
PYTHON_SCRIPT_LOG_DIR = "/home/root/PythonScriptLogs"

# Initialize logger
from logging.handlers import RotatingFileHandler

def configure_logger():
    """Configures and returns a logger instance with log rotation"""
    logger = logging.getLogger("FileManagementLogger")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        if not os.path.exists(PYTHON_SCRIPT_LOG_DIR):
            os.makedirs(PYTHON_SCRIPT_LOG_DIR)

        LOG_FILE = os.path.join(PYTHON_SCRIPT_LOG_DIR, "file_management_service.log")

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        file_handler = RotatingFileHandler(LOG_FILE, maxBytes=50*1024*1024, backupCount=3)
        file_handler.setLevel(logging.DEBUG)

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
        return result.returncode == 0
    except Exception as e:
        logger.error(f"Network check failed: {e}")
        return False

def find_application_binary():
    """Find the actual binary that starts with '5GCamera' in the current script directory"""
    try:
        script_directory = os.path.dirname(os.path.abspath(__file__))
        matching_files = glob.glob(os.path.join(script_directory, "5GCamera*"))

        for file in matching_files:
            if os.path.isfile(file) and os.access(file, os.X_OK):
                logger.info(f"Found application binary: {file}")
                return file

        logger.error("No valid '5GCamera*' binary found in the current directory!!!")
        return None

    except Exception as e:
        logger.error(f"Error finding application binary: {e}")
        return None

def start_application():
    """Start the application dynamically"""
    try:
        app_path = find_application_binary()

        if not app_path:
            logger.error("Cannot start application: No valid 5GCamera* binary found...!!!")
            return None

        if not os.path.exists(CAMERA_APP_LOG_DIR):
            os.makedirs(CAMERA_APP_LOG_DIR)

        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file_path = os.path.join(CAMERA_APP_LOG_DIR, f"application_{timestamp}.log")

        with open(log_file_path, "a") as log_file:
            process = subprocess.Popen(
                [app_path],
                stdout=log_file,
                stderr=log_file,
                preexec_fn=os.setsid
            )

        logger.info(f"Started application {app_path} with PID {process.pid}, logs: {log_file_path}")
        return process.pid

    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        return None

def get_running_pid():
    """Find the PID of the running application that starts with '5GCamera'"""
    try:
        result = subprocess.run(["pgrep", "-f", "5GCamera"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        pids = result.stdout.strip().split("\n")

        for pid in pids:
            if pid.isdigit():
                pid = int(pid)

                # Check if the process is a zombie
                ps_output = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                status = ps_output.stdout.strip()

                if "Z" in status:  # Check if process is defunct (zombie)
                    logger.warning(f"Process 5GCamera (PID {pid}) is a zombie! Restarting...")
                    return None  # Ignore zombie process

                return pid

        return None

    except Exception as e:
        logger.error(f"Error checking application status: {e}")
        return None

def monitor_application():
    """Ensure application is always running, with restart delay and retry limit"""
    retry_count = 0
    max_retries = 5

    while True:
        pid = get_running_pid()
        if not pid:
            logger.warning("5GCamera* is not running!!! Restarting...!!!")

            if retry_count >= max_retries:
                logger.error("Max retries reached. Not restarting application for now.")
                time.sleep(60)
                retry_count = 0

            else:
                start_application()
                retry_count += 1
                time.sleep(5)

        else:
            retry_count = 0

        time.sleep(5)

def wait_for_timestamp_file():
    """Wait indefinitely for system_timestamp.txt to appear"""
    while True:
        if os.path.exists(SYSTEM_TIMESTAMP_PATH):
            return True
        logger.warning("Waiting for timestamp file to appear...!!!")
        time.sleep(5)

def read_timestamp():
    """Read timestamp or fallback to current system time"""
    try:
        if os.path.exists(SYSTEM_TIMESTAMP_PATH):
            with open(SYSTEM_TIMESTAMP_PATH, 'r') as file:
                timestamp = file.readline().strip()
                return int(timestamp) if timestamp.isdigit() else int(time.time())
    except Exception as e:
        logger.error(f"Error reading timestamp: {e}")

    return int(time.time())

def generate_uuid_from_timestamp(timestamp):
    """Generate UUID based on timestamp"""
    return uuid.uuid1(node=timestamp)

def ensure_uuid_file():
    """Ensure system_uuid.txt exists, and create it if not"""
    if os.path.exists(SYSTEM_UUID_PATH):
        with open(SYSTEM_UUID_PATH, 'r') as file:
            existing_uuid = file.readline().strip()
            return existing_uuid if existing_uuid else generate_uuid_from_timestamp(read_timestamp())

    wait_for_timestamp_file()

    new_uuid = generate_uuid_from_timestamp(read_timestamp())
    with open(SYSTEM_UUID_PATH, 'w') as file:
        file.write(str(new_uuid))

    return str(new_uuid)

def main():
    """Main function to coordinate all tasks"""
    config = load_config(JSON_CONFIG_FILE)
    if not config:
        return

    server_ip = config.get("signalling_server_ip")
    if not is_network_available(server_ip):
        return

    pid = get_running_pid()
    if not pid:
        start_application()

    monitor_thread = threading.Thread(target=monitor_application, daemon=True)
    monitor_thread.start()

    ensure_uuid_file()

    while True:
        time.sleep(10)

if __name__ == "__main__":
    logger.info("Starting application monitor...")
    main()
