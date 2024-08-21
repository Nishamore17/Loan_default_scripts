from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import configparser
import json

class HiveConnector:
    def __init__(self, config_path):
        self.config_path = config_path
        with open(config_path, 'r') as json_file:
            self.config = json.load(json_file)
        
        self.spark = SparkSession.builder \
                .appName("MyPySparkApp") \
                .config("spark.sql.warehouse.dir", self.config['spark']['warehouse_dir']) \
                .config("spark.sql.catalogImplementation", "hive") \
                .config("hive.metastore.uris", self.config['spark']['metastore_uris']) \
                .enableHiveSupport() \
                .getOrCreate()

    def load_dataset(self):
        try:
            # Load the dataset
            self.input_location = self.config['path']['output_to_transform']
            print(f"Input Location: {self.input_location}")
            self.df = self.spark.read.parquet(self.input_location)
            self.df.printSchema()
        except Exception as e:
            print(f"An error occurred while loading the dataset: {str(e)}")

    def hive_connect(self):
        try:
            database_name = self.config['spark']['database_name']
            table_name = self.config['spark']['table_name']
            
            # Use the specified database
            self.spark.sql(f"create database if not exists {database_name}")
            self.spark.sql(f"use {database_name}")

            # If table does not already exist in Hive.
            if table_name not in self.spark.catalog.listTables():
                # Save DataFrame to Hive table
                self.df.write.mode("overwrite").saveAsTable(table_name)
            else:
                # Read the existing data from the Hive table
                existing_df = self.spark.table(table_name)

                # Identify existing keys
                existing_keys = existing_df.select('ID')

                # Filter out rows with existing keys from the new DataFrame
                new_unique_df = self.df.join(existing_keys, self.df['ID'] == existing_keys['ID'], "left_anti")

                # Append new unique rows to the Hive table
                new_unique_df.write.mode("append").saveAsTable(table_name)

                # Update existing rows in the Hive table
                existing_df.join(self.df, existing_df['ID'] == self.df['ID'], "inner") \
                    .select(existing_df.columns) \
                    .write.mode("overwrite").saveAsTable(table_name)

        except Exception as e:
            print(f"An error occurred while connecting to Hive: {str(e)}")

    def create_datamarts(self, df):
        try:
            database_name = self.config['spark']['database_name']
            # List of regions
            regions = ["central", "North", "North-East", "south"]
            for region in regions:
                # Filter data for the current region
                region_data = df.filter(df['Region'] == region)
                
                # Drop the 'Region' column
                region_data = region_data.drop('Region')
                
                # Create a Hive table for the data mart of the current region
                region_data.write.mode("overwrite").saveAsTable(f"{database_name}.datamart_{region.replace('-', '_')}")
        except Exception as e:
            print(f"An error occurred while creating data marts: {str(e)}")

if __name__ == "__main__":
    try:
        config_path = "/home/mavericbigdata12/Project_loan_default/config/config.json"  # Change the path to your config file
        hive_connector = HiveConnector(config_path)
        hive_connector.load_dataset()
        hive_connector.hive_connect()
        hive_connector.create_datamarts(hive_connector.df)
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
