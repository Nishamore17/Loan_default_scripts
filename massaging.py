import os
import json
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import avg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, when, regexp_replace,sha2,\
    date_format, upper, round as spark_round, udf, to_date, decode, encode, mean, stddev, abs

class DataMassaging:
    def __init__(self, config_path):
        with open(config_path, 'r') as json_file:
            self.config = json.load(json_file)
        self.spark = SparkSession.builder.appName("Data_Processing").getOrCreate()

    def load_dataset(self):
        # Load the dataset
        self.input_location = self.config['path']['input_to_massage']
        print(f"Input Location: {self.input_location}")
        self.df = self.spark.read.parquet(self.input_location)
        
    
    # 1. Validating Data
    def data_validation(self):
        try:
            if self.config["massaging"].get("data_validation"):
                        
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
            
            
    # 2. data_verification
    def data_verification(self):
        try:
            if self.config["massaging"].get("data_verification"):
                schema = self.config["spark_schema"]
                invalid_columns = []

                for field in schema:
                    column_name = field["name"]
                    expected_data_type = field["type"]
                    actual_data_type = self.df.schema[column_name].dataType

                    if expected_data_type != actual_data_type:
                        invalid_columns.append(column_name)
                if invalid_columns == []:
                    print("Columns with invalid datatypes are ",invalid_columns)
                else:
                    print("All columns datatype are correct.")
                return invalid_columns
        except Exception as e:
                print(f"Error in data_verification: {str(e)}")

    
    # 3. data_conversion
    def data_conversion(self):
        try:
            if self.config["massaging"].get("data_conversion"):
                columns2convert_data = self.config["columns2convert_data"]
                # Convert data types
                for c in columns2convert_data:
                    self.df = self.df.withColumn(c, col(c).cast(StringType()))
                    print(f"Data types for {c} is corrected.")
        except Exception as e:
                print(f"Error in data_conversion: {str(e)}")

                
    # 4. data formatting
    def data_formatting(self):
        try:
            if self.config["massaging"].get("data_formatting"):
                # Standardizing categorical column to uppercase
                for col_name in self.df.columns:
                    # Check if the column is categorical (StringType)
                    if isinstance(self.df.schema[col_name].dataType, StringType):
                        # Convert the values in the column to uppercase
                        self.df = self.df.withColumn(col_name, upper(col(col_name)))
        except Exception as e:
            print(f"Error in data_formatting: {str(e)}")

            
    # 5. Parse addresses
    def parse_address(self):
        try:
            if self.config["massaging"].get("parse_address"):
                self.df = self.df.withColumn("parsed_address", split(col(address_col), ",\s*"))
                self.df = self.df.withColumn("street_address", self.df.parsed_address.getItem(0))
                self.df = self.df.withColumn("city", self.df.parsed_address.getItem(1))
                self.df = self.df.withColumn("postal_code", self.df.parsed_address.getItem(2))
                self.df = self.df.drop("parsed_address")
                return self.df
        except Exception as e:
            print(f"Error in parse_address: {str(e)}")
            
    
    # 6. data cleaning      
    def data_cleaning(self):
        try:
            if self.config["massaging"].get("correct_typos_and_inaccuracies"):
                # Correct inaccuracies in specific columns
                corrections = self.config["typing_error_inaccuracies"]
                for col_name, mapping in corrections.items():
                    self.df = self.df.replace(to_replace=mapping, subset=[col_name])
        except Exception as e:
            print(f"Error in correct_typos_and_inaccuracies: {str(e)}")
            
    
    # 7. data_enrichment
    def data_enrichment(self):
        try:
            if self.config["massaging"].get("data_enrichment"):
                # Enrichment steps - Example: adding a new feature 'risk_score'
                loan_default_data['risk_score'] = loan_default_data['credit_score'] * loan_default_data['income']

                return loan_default_data
        except Exception as e:
            print(f"Error in data_enrichment: {str(e)}")
    
    # 8. data standardization
    def data_standardization(self):
        try:
            if self.config["massaging"].get("handle_special_characters_and_symbols"):
                # Function to check for special characters in a column
                def contains_special_characters(s):
                    # Define a regular expression pattern to match special characters
                    pattern = r'[^a-zA-Z0-9.\s]'
                    # Check if the column contains any special characters
                    return s.rlike(pattern)
 
                # Iterate over each column in the DataFrame
                for col_name in self.df.columns:
                    # Check if the column contains special characters
                    if self.df.select(col_name).filter(contains_special_characters(col(col_name))).count() > 0:
                        # Handle special characters by replacing them with an empty string
                        self.df = self.df.withColumn(col_name, regexp_replace(col(col_name), r'[^a-zA-Z0-9.\s]', ""))
        except Exception as e:
            print(f"Error in correct_typos_and_inaccuracies: {str(e)}")

    # 9. Data duplications
    def data_deduplication(self):
        try:
            if self.config["massaging"].get("remove_duplicates"):
                # Remove duplicate records
                self.df = self.df.dropDuplicates()
        except Exception as e:
            print(f"Error in remove_duplicates: {str(e)}")
            
    
    # 10. data_imputation
    def data_imputation(self):
        try:
            if self.config["massaging"]["data_imputation"]:          
                corrections = self.config["to_imputation"]
                for col_name, mapping in corrections.items():
                    self.df = self.df.withColumn(col_name, when(col(col_name).isNull(), mapping).otherwise(col(col_name)))
                print("Standardized categorical data.")
        except Exception as e:
            print(f"Error in remove_duplicates: {str(e)}")     


    #11 Data Normalization: Transform data into a common scale, such as normalizing numerical 
        #features to have a mean of 0 and a standard deviation of 1.
    def data_normalization(self):
        try:
            if self.config["massaging"].get("data_normalization"):
                normalization_columns = self.config["rounding_after_cleaning"]
                # Round Interest_rate_spread and LTV column to whole numbers in the same DataFrame
                for i in normalization_columns:
                    self.df = self.df.withColumn(i, spark_round(col(i)).cast("int"))
                print("Normalized to numerical column.")
        except Exception as e:
            print(f"An error occurred during data normalization: {str(e)}")
            
            
    # 12. Data Concatenation: Combine data from different sources or fields to create composite data, 
        #such as merging first and last names into a full name Age and Gender
    def concatenate_columns(self):
        try:
            if self.config["massaging"].get("concatenate_columns"):
                # Concatenate specified columns into a new column
                self.df = self.df.withColumn("new_col_name", concat(self.config['schema']['columns'][4], col(self.config['schema']['columns'][28])))
                print(f"Columns  concatenated into  successfully.")
        except Exception as e:
            print(f"Error in column concatenation: {str(e)}")
            
            
    # 13. Data Redaction: Mask or redact sensitive or personally identifiable information (PII) 
        #to ensure data privacy and compliance with regulations.#ID
    def redact_sensitive_info(self):
        try:
            if self.config["massaging"].get("redact_sensitive_info"):
                pii_columns = [self.config['schema']['columns'][1]]
                redacted_value = "REDACTED"

                # Apply redaction to the specified columns and create new columns for redacted data
                for column in pii_columns:
                    redacted_column = f"{column}_Redacted"
                    self.df = self.df.withColumn(redacted_column, lit(redacted_value))
        except Exception as e:
            print(f"An error occurred during data redaction: {str(e)}")
    
    # 14:Data Filtering: Remove irrelevant or unwanted data points or records from the dataset.
        # We have removed below 1% of null values 
    def data_filtering(self):
        try:
            if self.config["massaging"]["data_filtering"]:
                print("\n filtering data :")
                filter1 = self.config["filter1"]
                filtered_df = self.df[self.df[filter1["col_to_filter"]] > filter1["filter_value"]]
                filtered_df.show()
        except Exception as e:
            print(f"Error during data filtering: {str(e)}")
    
    
    
    # 15:Data Sampling: Select a representative subset of data for various purposes, such as model training or exploratory data analysis.
    def data_sampling(self):
        try:
            if self.config["massaging"]["data_sampling"]:
                print("\nData Sampling:")
                sampling_fraction = self.config.get('sampling_fraction', 0.1)
                sampled_df = self.df.sample(withReplacement=False, fraction=sampling_fraction)
                sampled_df.show()
        except Exception as e:
            print(f"Error in data sampling: {str(e)}")
            
        
    # 16. Data Subset Extraction: Extract specific portions or subsets of the data for focused analysis or reporting.
    def subset_extraction(self):
        try:
            if self.config["massaging"].get("subset_extraction"):
                column, value = self.config["Subset_column"]
                filtered_data = self.df.filter(self.df[column] == value)
                print("\nData Subset Extraction:")
                filtered_data.show()
                return filtered_data
        except Exception as e:
            print(f"An error occurred during subset extraction: {str(e)}")
            
    
    # 17. Data Transformation: Change the structure or representation of data, such as pivoting columns 
        #into rows or performing mathematical transformations.
    def transform_data(self):
        try:
            if self.config["massaging"].get("transform_data"):
                # Check if column is numeric
                column_name = self.config['data_transformation']['column_name']
                if self.df.schema[column_name].dataType in [DoubleType(), IntegerType()]:
                    # Perform data transformation
                    new_column_name = self.config['data_transformation']['new_column_name']
                    self.df = self.df.withColumn(new_column_name, col(column_name) / 2)
                    print("Data transformation completed.")
                else:
                    print("Data transformation skipped: Specified column is not numeric.")
        except Exception as e:
            print(f"An error occurred during data transformation: {str(e)}")
            
            
            
    # 18. Data Reconciliation: Ensure that data from different sources or systems align and match correctly.
    def reconcile_data(self):
        try:
            if self.config["massaging"].get("reconcile_data"):
                self.inconsistent_data = self.df.filter(
                    col(self.config['schema']['columns'][13]) - col(self.config['schema']['columns'][11]) != 0)
        except Exception as e:
            print(f"An error occurred during data reconciliation: {str(e)}")
            
    
    # 19. Data Aggregation: Combine and summarize data into a more compact form, often for reporting or analytics.
    def aggregate_data(self):
        try:
            if self.config["massaging"].get("aggregate_data"):
                # Group by age and calculate the mean of loan_amount
                self.df_age = self.df.groupBy(self.config['schema']['columns'][3]).agg(
                    avg(self.config['schema']['columns'][10]).alias("Mean_Loan_Amount"))
                # return self.df_age
        except Exception as e:
            print(f"An error occurred during data aggregation: {str(e)}")

            
    # 20. Data Versioning: Maintain a version history of the data to track changes and support data lineage​
    def data_versioning(self):
        try:
            if self.config["massaging"].get("data_versioning"):
                # Create a versioned file path based on the current timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                versioned_path = os.path.join(self.output_path, f"data_version_{timestamp}.parquet")

                # Write the DataFrame to the versioned path
                self.df.write.mode("overwrite").parquet(versioned_path)

                print(f"Data version saved at: {versioned_path}")
        except Exception as e:
            print(f"Error in version_data: {str(e)}")
            
            
    # Showing final dataset
    def show_df(self):
        self.df.show()
        return self.df
    
    # 
    def get_dataframe(self):
        try:
            op_path = self.config["path"]["output_to_massage"]
            self.df.write.save(op_path)
            print("Successfully save data.")
            return self.df
        except Exception as e:
            print(f"Error in get_dataframe: {str(e)}")


if __name__ == "__main__":
    
    config_path = "/home/mavericbigdata12/Project_loan_default/config/config.json"
    # Instantiate DataCleanser class
    Data_Massaging = DataMassaging(config_path)

    # Load the dataset
    Data_Massaging.load_dataset()

    print("Data massaging started...")
    Data_Massaging.data_validation()
    Data_Massaging.data_verification()
    Data_Massaging.data_conversion()
    Data_Massaging.data_formatting()
    Data_Massaging.parse_address()
    
    Data_Massaging.data_cleaning()
    Data_Massaging.data_enrichment()
    Data_Massaging.data_standardization()
    Data_Massaging.data_deduplication()
    Data_Massaging.data_imputation()
    
    Data_Massaging.data_normalization()
    Data_Massaging.concatenate_columns()
    Data_Massaging.redact_sensitive_info()
    Data_Massaging.data_filtering()
    Data_Massaging.data_sampling() 
    
    Data_Massaging.transform_data()
    Data_Massaging.subset_extraction()
    Data_Massaging.reconcile_data()
    Data_Massaging.aggregate_data()
    Data_Massaging.data_versioning()
    
    
    
    Data_Massaging.show_df()
    massaged_df = Data_Massaging.get_dataframe()
    Data_Massaging.spark.stop()