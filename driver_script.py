import subprocess
from pyspark.sql import SparkSession
import json
from datetime import datetime
import os
import logging



class DataProcessingPipeline:

    def __init__(self, config_file):
        # Reading JSON config
        with open(config_file, 'r') as json_file:
            self.config = json.load(json_file)

        # Specify the log file path
        self.log_file_dir = f'Log_files_{datetime.now().strftime("%Y%m%d%H%M%S")}'
        os.makedirs(self.log_file_dir, exist_ok=True)  # Create directory if it doesn't exist
        self.log_file_path = os.path.join(self.log_file_dir, 'Log_file.txt')

        # Initialize Spark session
        self.spark = SparkSession.builder.appName("MainScript1").getOrCreate()

        # Configure logging
        logging.basicConfig(filename=self.log_file_path, level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

    def print_current_datetime(self,message=""):
        now = datetime.now()
        current_datetime = now.strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"{message} {current_datetime}"
        logging.info(log_message)
        print(log_message)

    def run_cleansing_phase(self):
        try:
            # Step 1: Call cleansing script
            logging.info("****************CLEANSING PHASE STARTED*****************")
            self.print_current_datetime()
            cleansing_script = self.config['path']['cleanse_script']
            subprocess.run(["python", cleansing_script, "--input_path", self.config['path']['data_file_path']],
                           stdout=open(self.log_file_path, 'a'), stderr=subprocess.STDOUT)
            self.print_current_datetime()
            logging.info("******************CLEANSING PHASE COMPLETED*******************")
        except Exception as e:
            self.log_error(e)

    def run_hdfs_phase(self):
        try:
            # Step 2: Call Local to HDFS script
            logging.info("****************MOVING TO HDFS PHASE STARTED*****************")
            self.print_current_datetime()
            hdfs_script = self.config['path']['hdfs_script']
            subprocess.run(["python", hdfs_script, "--input_path", self.config['path']['dir_to_wacth']],
                           stdout=open(self.log_file_path, 'a'), stderr=subprocess.STDOUT)
            self.print_current_datetime()
            logging.info("******************HDFS DATA MOVEMENT COMPLETED*******************")
        except Exception as e:
            self.log_error(e)

    def run_massaging_phase(self):
        try:
            # Step 3: Call massaging script
            logging.info("****************MASSAGING PHASE STARTED*****************")
            self.print_current_datetime()
            massaging_script = self.config['path']['massage_script']
            subprocess.run(["python", massaging_script, "--input_path", self.config['path']['input_to_massage']],
                           stdout=open(self.log_file_path, 'a'), stderr=subprocess.STDOUT)
            self.print_current_datetime()
            logging.info("******************MASSAGING PHASE COMPLETED*******************")
        except Exception as e:
            self.log_error(e)

    def run_transformation_phase(self):
        try:
            # Step 4: Call transformation script
            logging.info("****************TRANSFORMING PHASE STARTED*****************")
            self.print_current_datetime()
            transforming_script = self.config['path']['transform_script']
            subprocess.run(["python", transforming_script, "--input_path", self.config['path']['input_to_transform']],
                           stdout=open(self.log_file_path, 'a'), stderr=subprocess.STDOUT)
            self.print_current_datetime()
            logging.info("*****************TRANSFORMATION PHASE COMPLETED******************")
            logging.info("PARQUET FILES ARE NOW READY TO BE MOVED TO HIVE")
        except Exception as e:
            self.log_error(e)

    def run_hive_phase(self):
        try:
            # Step 5: Call Hive script
            logging.info("****************HIVE TABLE CREATION STARTED*****************")
            self.print_current_datetime()
            hive_table_script = self.config['path']['hive_script']
            subprocess.run(["python", hive_table_script, "--input_path", self.config['path']['output_to_transform']],
                           stdout=open(self.log_file_path, 'a'), stderr=subprocess.STDOUT)
            self.print_current_datetime()
            logging.info("*****************TABLE CREATION COMPLETED******************")
        except Exception as e:
            self.log_error(e)

    def cleanup(self):
        # Stop the Spark session
        self.spark.stop()


if __name__ == "__main__":
    config_path = "/home/mavericbigdata12/Project_loan_default/config/config.json"
    pipeline = DataProcessingPipeline(config_path)

    print("****************MOVING TO HDFS PHASE STARTED*****************")
    pipeline.print_current_datetime("Before Data movement")
    pipeline.run_hdfs_phase()
    pipeline.print_current_datetime("After Data movement")
    print("****************DATA LOADED INTO HDFS*****************")
    
    print("****************CLEANSING PHASE STARTED*****************")
    pipeline.print_current_datetime("Before run_cleansing_phase")
    pipeline.run_cleansing_phase()
    pipeline.print_current_datetime("After run_cleansing_phase")
    print("******************CLEANSING PHASE COMPLETED*******************")
          
    print("****************MASSAGING PHASE STARTED*****************")
    pipeline.print_current_datetime("Before Massaging Phase")
    pipeline.run_massaging_phase()
    pipeline.print_current_datetime("After Massaging Phase")
    print("****************MASSAGING PHASE COMPLETED*****************")
          
    print("****************TRANSFORMING PHASE STARTED*****************")
    pipeline.print_current_datetime("Before Transforming Phase")
    pipeline.run_transformation_phase()
    pipeline.print_current_datetime("After Transforming Phase")
    print("****************TRANSFORMING PHASE COMPLETED*****************")      
    
    print("****************HIVE TABLE CREATION STARTED*****************")
    pipeline.print_current_datetime("Before Hive table creation")
    pipeline.run_hive_phase()
    pipeline.print_current_datetime("After Hive table creation")
    print("****************TABLE CREATION COMPLETED*****************")
    

    pipeline.cleanup()