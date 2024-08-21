import os
import subprocess
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import json
config_path = "/home/mavericbigdata12/Project_loan_default/config/config.json"
# Read configuration from JSON file
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

watched_directory1 = config['path']['watched_directory']
log_file = config['path']['log_file']
wrapper_script = config['path']['wrapper_script']

# Configure logging
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        logging.info(f'File {event.src_path} has been found. Running validation...')
        self.run_validation_script(event.src_path)

    def run_validation_script(self, file_path):
        script_path = wrapper_script
        logging.info(f'Running validation script: {script_path} with file: {file_path}')
        print(file_path)
        # Run the validation script and capture stdout and stderr
        result = subprocess.run([script_path, file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Log the stdout and stderr to a file
        logging.info(f'Stdout:\n{result.stdout}')
        logging.error(f'Stderr:\n{result.stderr}')
        print(result.stdout)
        if result.stderr:
            print(result.stderr)

def main():
    # Directory to watch
    watched_directory = watched_directory1

    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path=watched_directory, recursive=False)
    observer.start()

    try:
        logging.info("Monitoring")
        print("Monitoring")
        while True:
            time.sleep(3)  # Sleep for 3 second and continue watching
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

if __name__ == "__main__":
    main()