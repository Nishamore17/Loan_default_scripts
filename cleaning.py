from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace,sha2,\
    date_format, upper, round as spark_round, udf, to_date, decode, encode, mean, stddev, abs
from pyspark.sql.types import StringType
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import json

class DataCleanser:
    def __init__(self, config_path):
        with open(config_path, 'r') as json_file:
            self.config = json.load(json_file) 
                   
        self.spark = SparkSession.builder.appName("DataCleansing").getOrCreate()
        self.geolocator = Nominatim(user_agent="address_validator")

    def load_dataset(self):
        # Load the dataset
        self.input_location = self.config['path']['data_file_path']
        print(f"Input Location: {self.input_location}")
        self.df = self.spark.read.csv(self.input_location, header=True, inferSchema=True)


    # 1. Removing duplicate values
    def remove_duplicates(self):
        try:
            if self.config["cleansing_config"].get("remove_duplicates"):
                # Remove duplicate records
                self.df = self.df.dropDuplicates()
                print("Duplicates removed.")
        except Exception as e:
            print(f"Error in remove_duplicates: {str(e)}")


    # 2. Handling Missing Values - Filling null values as None
    def handle_missing_values(self):
        try:
            if self.config["cleansing_config"].get("handle_missing_values"):
                # Filling missing values with None
                for col_name in self.df.columns:
                    self.df = self.df.withColumn(col_name, when(col(col_name) == "", None).otherwise(col(col_name)))
                print("Handled missing values.")
        except Exception as e:
            print(f"Error in handle_missing_values: {str(e)}")

    # 3. Standardizing Data
    def standardize_data(self):
        try:
            if self.config["cleansing_config"].get("standardize_data"):
                # Standardizing categorical column to uppercase
                for col_name in self.df.columns:
                    # Check if the column is categorical (StringType)
                    if isinstance(self.df.schema[col_name].dataType, StringType):
                        # Convert the values in the column to uppercase
                        self.df = self.df.withColumn(col_name, upper(col(col_name)))
                print("Standardized Data.")
        except Exception as e:
            print(f"Error in standardize_data: {str(e)}")
    

    # 4. Correcting Typos and Inaccuracies #change
    def correct_typos_and_inaccuracies(self):
        try:
            if self.config["cleansing_config"].get("correct_typos_and_inaccuracies"):
                # Correct inaccuracies in specific columns
                corrections = self.config["typing_error_inaccuracies"]
                for col_name, mapping in corrections.items():
                    self.df = self.df.replace(to_replace=mapping, subset=[col_name])
                print("Corrected typos and Inaccuracies")
        except Exception as e:
            print(f"Error in correct_typos_and_inaccuracies: {str(e)}")


    # 5. Addressing Outliers
    def handle_outliers(self):
        try:
            if self.config["cleansing_config"].get("handle_outliers"):
                columns_to_winsorize = self.config["columns_to_winsorize"]
                threshold=self.config["threshold"]
                # Winsorize each specified column
                for col_name in columns_to_winsorize:
                    # Calculate mean and standard deviation
                    summary_stats = self.df.select(mean(col_name), stddev(col_name))
                    mean_val = summary_stats.first()[0]
                    std_dev = summary_stats.first()[1]

                    # Calculate z-scores
                    z_score = abs((col(col_name)-mean_val)/std_dev)

                    # Define winsorized values based on threshold
                    winsorized_col = when(z_score > threshold, 
                                        mean_val + threshold * std_dev).otherwise(col(col_name))

                    # Update the DataFrame with winsorized column
                    self.df = self.df.withColumn(col_name, winsorized_col)
                print(f"Winsorized columns :{columns_to_winsorize}")

        except Exception as e:
            print(f"Error in handle_outliers: {str(e)}")
    
    # 6. Handling Inconsistent Data
    def handle_inconsistent_data(self):
        try:
            if self.config["cleansing_config"].get("handle_inconsistent_data"):
                inconsistant_column = self.config["inconsistant_column"]
                # Round Interest_rate_spread and LTV column to whole numbers in the same DataFrame
                for i in inconsistant_column:
                    self.df = self.df.withColumn(i, spark_round(col(i),2))
                print("Handled Inconsistant data.")
        except Exception as e:
            print(f"Error in handle_inconsistent_data: {str(e)}")


    # 7. Dealing with Incomplete Records
    def handle_incomplete_records(self):
        try:
            if self.config["cleansing_config"].get("handle_incomplete_records"):
                # Count the number of null values in each row
                null_threshold = self.config["null_threshold"]
                self.df = self.df.dropna(thresh=(null_threshold))
                print("Removed incomplete records.")
        except Exception as e:
            print(f"Error in handle_incomplete_records: {str(e)}")


    # 8. Converting Data Types
    def convert_data_types(self):
        try:
            if self.config["cleansing_config"].get("convert_data_types"):
                        
                columns2convert_data = self.config["columns2convert_data"]
                # Convert data types
                for c in columns2convert_data:
                    self.df = self.df.withColumn(c, col(c).cast(StringType()))
                    print(f"Data types for {c} is corrected.")
        except Exception as e:
            print(f"Error in convert_data_types: {str(e)}")

    # 9. Normalizing Data
    def normalize_data(self): 
        try:
            if self.config["cleansing_config"].get("normalize_data"):
                # Iterate over all string columns and apply trim function
                for col_name in self.df.columns:
                    if self.df.schema[col_name].dataType == "string":
                        self.df = self.df.withColumn(col_name, col(col_name).trim())
                print("Normalized string data- removed white spaces.")
        except Exception as e:
            print(f"Error in normalize_data: {str(e)}")

    # 10. Standardizing Categorical Data 
    def standardize_categorical_data(self):
        try:
            if self.config["cleansing_config"].get("standardize_categorical_data"):
                # Correct inaccuracies in specific columns
                corrections = self.config["standardizing_column"]
                for col_name, mapping in corrections.items():
                    self.df = self.df.replace(to_replace=mapping, subset=[col_name])
                print("Standardized categorical data.")
        except Exception as e:
            print(f"Error in standardize_categorical_data: {str(e)}")

            
    # 11. Validating Data
    def validate_data(self):
        try:
            if self.config["cleansing_config"].get("validate_data"):
                        
                # Iterate over selected columns for validation
                for column in self.df.columns:
                    print(f"Validation for column: {column}")
                    
                    # Check for null values
                    null_count = self.df.filter(col(column).isNull()).count()
                    print(f"Null count: {null_count}")

                    # Check for unique values
                    unique_count = self.df.select(column).distinct().count()
                    print(f"Unique count: {unique_count}")

                    # Check for data type consistency
                    data_type = self.df.schema[column].dataType
                    print(f"Data type: {data_type}")

                    # Check for data format (specifically for the 'year' column)
                    if column == 'year':
                        invalid_format_count = self.df.filter(~col(column).cast("string").rlike(r"^\d{4}$")).count()
                        print(f"Invalid format count: {invalid_format_count}")

                    # Format check for Gender column
                    if column == 'Gender':
                        invalid_gender_count = self.df.filter(~col(column).isin(["Male", "Female", "Joint", "Sex Not Available"])).count()
                        print(f"Invalid gender count: {invalid_gender_count}")

                    print("\n")
                print("Validation Ended")
        except Exception as e:
            print(f"Error in validate_data: {str(e)}")

    # 12. Addressing Inconsistencies in Date and Time Formats
    def address_inconsistencies_in_date_time_formats(self):
        try:
            if self.config["cleansing_config"].get("address_inconsistencies_in_date_time_formats"):
                        
                # Standardize date format for the 'datetime' column
                for date in self.config["datetime_columns"]:
                    self.df = self.df.withColumn(date, to_date(col(date), 'yyyy'))
                print("Standardized date format.")
        except Exception as e:
            print(f"Error in address_inconsistencies_in_date_time_formats: {str(e)}")


    # 13. Removing Irrelevant Information
    def remove_irrelevant_information(self):
        try:
            if self.config["cleansing_config"].get("remove_irrelevant_information"):
                irrelevant_columns = self.config["irrelevant_columns"]
                self.df = self.df.drop(*irrelevant_columns)
                print("Dropped irrelevant columns.")
        except Exception as e:
            print(f"Error in remove_irrelevant_information: {str(e)}")

    # 14. Handling Encoding Issues
    def handle_encoding_issues(self):
        try:
            if self.config["cleansing_config"].get("handle_encoding_issues"):
                        
                encoding=self.config["Text_encoding"]
                # Iterate over each column in the DataFrame
                for col_name in self.df.columns:
                    # Check if the column is of StringType (text data)
                    if self.df.schema[col_name].dataType == StringType():
                        # Apply the encoding UDF to the column
                        self.df = self.df.withColumn(col_name, decode(encode(col(col_name) , encoding), encoding))
                print("Handled text encoding issues.")
        except Exception as e:
            print(f"Error in handle_encoding_issues: {str(e)}")

    # 15. Dealing with Inconsistent Units (changing Upfront_charges from float to int)
    def deal_with_inconsistent_units(self):
        try:
            if self.config["cleansing_config"].get("deal_with_inconsistent_units"):
                inconsistant_unit_column = self.config["inconsistant_unit_column"]
                # Round Upfront_charges column to whole numbers in the same DataFrame
                for i in inconsistant_unit_column:
                    self.df = self.df.withColumn(i, spark_round(col(i)).cast("int"))
                print("Dealt with inconsistant units.")
        except Exception as e:
            print(f"Error in deal_with_inconsistent_units: {str(e)}")

    # 16. resolve_data_integration_issues
    def resolve_data_integration_issues(self):
        try:
            if self.config["cleansing_config"].get("resolve_data_integration_issues"):
                corrections = self.config["integration_issue_col"]
                for col_name, mapping in corrections.items():
                    self.df = self.df.replace(to_replace=mapping, subset=[col_name])
                print("Resolved data integration.")
        except Exception as e:
            print(f"Error in resolve_data_integration_issues: {str(e)}")

    # 17. Validating References and Foreign Keys
    def validate_references_and_foreign_keys(self):
        try:
            if self.config["cleansing_config"].get("validate_references_and_foreign_keys"):
                reference_df = self.config["reference_df"]
                foreign_key_columns =self.config["foreign_key_columns"]
                for column in foreign_key_columns:
                    # Check for invalid foreign key values
                    invalid_foreign_keys = self.df.join(reference_df, self.df[column] == reference_df['id'], 'left_anti').count()
                    
                    if invalid_foreign_keys == 0:
                        print(f"No invalid foreign key values found in column '{column}'.")
                    else:
                        print(f"Found {invalid_foreign_keys} invalid foreign key values in column '{column}'.")
                print("Validated references and foreign keys.")
        except Exception as e:
            print(f"Error in validate_references_and_foreign_keys: {str(e)}")

    # 18. Geocoding and Address Validation
    def geocode_and_address_validation(self):
        if self.config["cleansing_config"].get("geocode_and_address_validation"):  
            try:
                location = self.geolocator.geocode(self.config["address"])
                if location:
                    print(location.latitude, location.longitude)
                else:
                    print(None, None)
            except GeocoderTimedOut:
                return None, None



    # 19. Handling Special Characters and Symbols   
    def handle_special_characters_and_symbols(self):
        try:
            if self.config["cleansing_config"].get("handle_special_characters_and_symbols"):
                # Function to check for special characters in a column
                def contains_special_characters(s):
                    # Define a regular expression pattern to match special characters
                    pattern = r'[^a-zA-Z0-9.\s]'
                    # Check if the column contains any special characters
                    return s.rlike(pattern)

                # Iterate over each column in the DataFrame
                for col_name in self.df.columns:
                    # Check if the column contains special characters
                    if self.df.select(col_name).filter(contains_special_characters(col(col_name))).count() > 0 and col_name != "age":
                        # Handle special characters by replacing them with an empty string
                        self.df = self.df.withColumn(col_name, regexp_replace(col(col_name), r'[^a-zA-Z0-9.\s]', ""))
                print("Handled special characters.")
        except Exception as e:
            print(f"Error in handle_special_characters_and_symbols: {str(e)}")
       

    # 20. Conforming to Data Privacy Regulations
    def conform_to_data_privacy_regulations(self):
        try:
            if self.config["cleansing_config"].get("conform_to_data_privacy_regulations"):
                columns_to_hash=  self.config['hashing_privacy']['columns_to_hash']
                hash_length= self.config['hashing_privacy']['hash_length']
                for column in columns_to_hash:
                    self.df = self.df.withColumn(column, sha2(col(column).cast("string"), hash_length))
                print("Hashed sesitive data.")
        except Exception as e:
            print(f"Error in conform_to_data_privacy_regulations: {str(e)}")

    #  display Dataframe
    def display_dataset(self):
        # Display the modified dataset
        self.df.show()
        return self.df


    # Get Dataframe
    def get_dataframe(self):
        try:
            op_path = self.config["path"]["output_path_clean"]
            self.df.write.option("header", True).save(op_path)
            
        except Exception as e:
            print(f"Error in get_dataframe: {str(e)}")

if __name__ == "__main__":
    
    config_path = "/home/mavericbigdata12/Project_loan_default/config/config.json"
    # Instantiate DataCleanser class
    data_cleanser = DataCleanser(config_path)

    # Load the dataset
    data_cleanser.load_dataset()

    #Calling Cleaning methods
    data_cleanser.remove_duplicates()
    data_cleanser.handle_missing_values()
    data_cleanser.standardize_data()
    data_cleanser.correct_typos_and_inaccuracies()
    data_cleanser.handle_outliers()
    data_cleanser.handle_inconsistent_data()
    data_cleanser.handle_incomplete_records()
    data_cleanser.convert_data_types()
    data_cleanser.normalize_data()
    data_cleanser.standardize_categorical_data()
    data_cleanser.validate_data()
    data_cleanser.address_inconsistencies_in_date_time_formats()
    data_cleanser.remove_irrelevant_information()
    data_cleanser.handle_encoding_issues()
    data_cleanser.deal_with_inconsistent_units()
    data_cleanser.resolve_data_integration_issues()
    data_cleanser.validate_references_and_foreign_keys()
    data_cleanser.geocode_and_address_validation()
    data_cleanser.handle_special_characters_and_symbols()
    data_cleanser.conform_to_data_privacy_regulations()
    
    # Display the modified dataset
    cleaneddf = data_cleanser.display_dataset()
    data_cleanser.get_dataframe()