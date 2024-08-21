from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from pyspark.sql.functions import col, when, regexp_replace,sha2,\
    #date_format, upper, round as spark_round, udf, to_date, decode, encode, mean, stddev, abs
#from pyspark.sql.types import StringType
#from pyspark.sql.functions import col, min, max, round, year, current_date
#from pyspark.sql import functions as F


from pyspark.ml.feature import StringIndexer
import pandas as pd
import json


class DataTransform:
        def __init__(self, config_path):
                with open(config_path, 'r') as json_file:
                    self.config = json.load(json_file)            
                self.spark = SparkSession.builder.appName("DataTransform").getOrCreate()

        def load_dataset(self):
            # Load the dataset
            self.input_location = self.config['path']['input_to_transform']
            print(f"Input Location: {self.input_location}")
#             self.df = self.spark.read.option("header", "true").option("inferschema", "true").option("sep",",").option("nullValue","NA").csv(self.input_location)
            self.df = self.spark.read.parquet(self.input_location)
            self.df.printSchema() 

    # 1.Data Aggregation 
        def data_aggregation(self):
            try:
                if self.config["transforming"]["data_aggregation"]:
                    print("\nData Describe:")
                    col_to_describe = self.config['col_to_describe']
                    for col_name in col_to_describe:
                        self.df.select(col_name).describe().show()
                        #self.df.describe().show()     #for complete records in one table
            except Exception as e:
                print(f"Error in data_aggregation: {str(e)}")

    # 2. Data Filtering 
        def data_filtering(self):
            try:
                if self.config["transforming"]["data_filtering"]:
                    print("\n removing null values :")
                    null_counts = {col: self.df.filter(self.df[col].isNull()).count() for col in self.df.columns}
                    for col, count in null_counts.items():
                        percentage = (count / self.df.count()) * 100  # Calculate the percentage
                        if 0 < percentage < 1:  # Check if percentage is greater than 0 and less than 1
                            self.df = self.df.dropna(subset=[col])  # Drop rows with null values in this column
                    print(f"after removing null values rows count is : {self.df.count()}")
            except Exception as e:
                print(f"Error in data_filtering: {str(e)}")
    
    # 3. Data Sorting
        def data_sorting(self):
            try:
                if self.config["transforming"]["data_sorting"]:
                    print("\n data sorting by loan amount by descending order :")
                    col_to_sort=self.config["col_to_sort"]
                    sorted_df = self.df.orderBy(self.df[col_to_sort].desc())
                    #sorted_df.show()
            except Exception as e:
                print(f"Error in data_sorting: {str(e)}")
        
    # 4. Data Joining and Merging
        def data_joining_and_merging(self):
            try:
                if self.config["transforming"]["data_joining_and_merging"]:
                    print("\nData Joining and Merging:")
                    try:
                        df1 =self.config.get('df1', None)
                        df2 =self.config.get('df2', None)
                        common_column = self.config.get('join_common_column', None)
                        join_type = self.config.get('join_type', 'inner')
                        if common_column:
                            merged_df = df1.join(df2, on=common_column, how=join_type)
                            merged_df.show()
                        else:
                            print("Error: Common column not specified in config.")
                    except Exception as e:
                        print(f"Error in data joining and merging: {str(e)}")
            except Exception as e:
                print(f"Error in data_joining_and_merging: {str(e)}")
            
    # 5. Data Splitting
        def data_splitting(self):
            try:
                if self.config["transforming"]["data_splitting"]:
                    print("\n Data Splitting:")
                    split_by_status=self.config["split_by_status"]
                    for status in split_by_status:
                        split_df = self.df.filter(self.df['Status'] == status)
                        print(f"Subset for status '{status}':")
                        split_df.show()
                        print("\n")
            except Exception as e:
                print(f"Error in data_splitting: {str(e)}")
            
    # 6. Data Reshaping
        def data_reshaping(self):
            try:
                if self.config["transforming"]["data_reshaping"]:
                    print("\n data reshaping :")
                    reshaping_col = self.config["reshaping_col"]
                    pivoted_df = self.df.groupby(reshaping_col["col"]).pivot(reshaping_col["pivot_by"]).sum(reshaping_col["col_to_agg"])
                    pivoted_df.show()
            except Exception as e:
                print(f"Error in data_reshaping: {str(e)}")
        
    # 7. Data Transposition
        def data_transposition(self):
            try:
                if self.config["transforming"]["data_transposition"]:
                    print("\n Data Transposition :")
                    df1 = self.df.toPandas()
                    transposed_df = df1.transpose()
                    print(transposed_df)
            except Exception as e:
                print(f"Error in data_transposition: {str(e)}")
        
    # 8. Data imputation
        def data_imputation(self):
            try:
                if self.config["transforming"]["data_imputation"]:
                    print("\n data imputation :")
                    col_to_fillna=self.config["col_to_fillna"]
                    self.df[col_to_fillna].fillna(self.df[col_to_fillna].mean(), inplace=True)
            except Exception as e:
                print(f"Error in data_imputation: {str(e)}")
                
    # 9. Data Normalization
        def data_normalization(self):
            try:
                if self.config["transforming"]["data_normalization"]:
                    print("\n data normalization :")
                    col_to_normalize = self.config["col_to_normalize"]
                    # Calculating min and max values for each numeric column
                    min_values = self.df.select([min(col(col_name)).alias(col_name) for col_name in col_to_normalize])
                    max_values = self.df.select([max(col(col_name)).alias(col_name) for col_name in col_to_normalize])

                    # Calculating normalized values for each numeric column
                    normalized_exprs = [
                    ((col(col_name) - min_values.first()[col_name]) / 
                    (max_values.first()[col_name] - min_values.first()[col_name])).alias(col_name + '_normalized') 
                    for col_name in col_to_normalize
                    ]

                    # Applying the normalization expressions to the DataFrame
                    normalized_df = self.df.select(['ID', 'year'] + normalized_exprs)  # Add any non-numeric columns you want to keep
                    normalized_df.show()
            except Exception as e:
                print(f"Error in remove_duplicates: {str(e)}")
                
        
    # 10. Data Encoding
        def data_encoding(self):
            try:
                if self.config["transforming"]["data_encoding"]:
                    print("\nData Encoding:")
                    encoding_config = self.config.get('data_encoding', {})
                    for column, encoding_map in encoding_config.items():
                        when_conditions = [when(col(column) == value, encoded_value).otherwise(0).alias(f"{column}_encoded_{encoded_value}") 
                                           for value, encoded_value in encoding_map.items()]
                        encoded_df = self.df.select("*", *when_conditions)
                        encoded_df.show()  
            except Exception as e:
                print(f"Error in data_encoding: {str(e)}")
        
            
    # 11. Data Scaling
        def data_scaling(self):
            try:

                if self.config["transforming"]["data_scaling"]:
                    print("\nData Scaling:")
                    scaling_config = self.config.get('scaling_columns', {})
                    for column, _ in scaling_config.items():
                        min_val, max_val = self.df.selectExpr(f"min({column})", f"max({column})").first()
                        scale_factor = 1 / (max_val - min_val)
                        self.df = self.df.withColumn(column, (col(column) - min_val) * scale_factor)
                    self.df.show() if scaling_config else print("Error: Scaling columns and factors not specified in config.")
            except Exception as e:
                print(f"Error in data_scaling: {str(e)}")
        
    # 12. Data Aggregation Timeframes
        def data_agg_timeframes(self):
            try:
                if self.config["transforming"]["data_agg_timeframes"]:
                    print("\nData Transformation by Calculations:")
                    try:
                        transformation_columns = self.config.get('transformation_columns', {})
                        if transformation_columns:
                            for new_column, calculation in transformation_columns.items():
                                self.df = self.df.withColumn(new_column, eval(calculation))
                            self.df.show()
                        else:
                            print("Error: Transformation columns and calculations not specified in config.")
                    except Exception as e:
                        print(f"Error in data transformation by calculations: {str(e)}")
            except Exception as e:
                print(f"Error in data_agg_timeframes: {str(e)}")
   
    #13. Data Binning
        def data_binning(self):
            try:
                if self.config["transforming"]["data_binning"]:

                    print("\n data binning :")
                    a = config["cols_to_binning"].keys()
                    for i in a:
                        bin_boundaries = self.config["cols_to_binning"][i][0]

                        bin_labels = self.config["cols_to_binning"][i][1]

                        # Create a Bucketizer transformer
                        bucketizer = Bucketizer(splits=bin_boundaries, inputCol='Credit_Score', outputCol=f'{list(config["cols_to_binning"].keys())[0]}_bins')

                        # Apply the Bucketizer to your DataFrame
                        binned_df = bucketizer.transform(self.df)

                        # Map the bin indices to bin labels
                        binned_df = binned_df.withColumn(f'{list(config["cols_to_binning"].keys())[0]}_bins', 
                                                         when(col(f'{list(config["cols_to_binning"].keys())[0]}_bins') == 0, bin_labels[0])
                                                         .when(col(f'{list(config["cols_to_binning"].keys())[0]}_bins') == 1, bin_labels[1])
                                                         .when(col(f'{list(config["cols_to_binning"].keys())[0]}_bins') == 2, bin_labels[2])
                                                         .when(col(f'{list(config["cols_to_binning"].keys())[0]}_bins') == 3, bin_labels[3])
                                                         .when(col(f'{list(config["cols_to_binning"].keys())[0]}_bins') == 4, bin_labels[4])
                                                         .otherwise('Unknown'))

                        # Display the updated DataFrame with credit score bins
                        binned_df.select("credit_score",f'{list(config["cols_to_binning"].keys())[0]}_bins').show(2)
            except Exception as e:
                print(f"Error in data_binning: {str(e)}")
                
                
    # 14. Data Extraction 
        def data_extraction(self):
            try:
                if self.config["transforming"]["data_extraction"]:
                    print("\n data extraction :")
                    col_to_extract=self.config["col_to_extract"]
                    extracted_df = self.df[col_to_extract]
                    extracted_df.show()
                    return extracted_df
            except Exception as e:
                print(f"Error in data_extraction: {str(e)}")
    
    # 15. Data Duplication
        def data_duplication(self):  
            try:
                if self.config["transforming"]["data_duplication"]:
                    print("\n Data Duplication :")
                    # Check for duplicates based on all columns in the DataFrame
                    duplicates_count = self.df.count() - self.df.dropDuplicates().count()

                    # If duplicates are found, drop them and show the cleaned DataFrame
                    if duplicates_count > 0:
                        cleaned_df = self.df.dropDuplicates()
                        print(f"{duplicates_count} duplicates found and dropped.")
#                         cleaned_df.show()
                    else:
                        print("No duplicates found.")
            except Exception as e:
                print(f"Error in data_duplication: {str(e)}")

            
     # 16.Data Sampling
        def data_sampling(self):
            try:

                if self.config["transforming"]["data_sampling"]:
                    print("\nData Sampling:")
                    try:
                        sampling_fraction = self.config.get('sampling_fraction', 0.1)
                        sampled_df = self.df.sample(withReplacement=False, fraction=sampling_fraction)
                        sampled_df.show()
                    except Exception as e:
                        print(f"Error in data sampling: {str(e)}")
            except Exception as e:
                print(f"Error in remove_duplicates: {str(e)}")
                
    # 17. Data Transformation by Calculations
        def data_transform_by_cal(self):
            try:
                if self.config["transforming"]["data_transform_by_cal"]:
                    print("\n Data Transform by calculation:")
                    newdf = self.df.withColumn("Age_Lower", col("submission_of_application").substr(1, 2).cast("int")).withColumn("Age", year(current_date()) - col("Age_Lower"))
                    newdf.show()
            except Exception as e:
                print(f"Error in data_transform_by_cal: {str(e)}")

    
    # 18. Data Conversion 
        def data_conversion(self): 
            try:
                if self.config["transforming"]["data_conversion"]:
                    print("\n Data conversion:")
                    # Convert loan_amount from INR to USD using a conversion rate of 1 INR = 0.012 USD
                    con_df = self.df.withColumn("loan_amount_usd", round(col("loan_amount") * 0.012, 2))
#                     con_df.show()
            except Exception as e:
                print(f"Error in data_conversion: {str(e)}")
                    
    # 19. Data Parsing
        def data_parsing(self):
            try:
                if self.config["transforming"]["data_parsing"]:
                    print("\nData Parsing:")
                    try:
                        parsing_config = self.config.get('parsing_config', {})
                        if parsing_config:
                            for column, parsing_pattern in parsing_config.items():
                                # Perform parsing based on parsing pattern
                                pass  # Placeholder for actual parsing logic
                        else:
                            print("Error: Parsing config not specified in config.")
                    except Exception as e:
                        print(f"Error in data parsing: {str(e)}")
            except Exception as e:
                print(f"Error in data_parsing: {str(e)}")

    # 20. Data Redundancy Removal
        def data_redundancy_removal(self):
            try:
                if self.config["transforming"]["data_redundancy_removal"]:
                    print("\nData Redundancy Removal:")
                    try:
                        redundancy_columns = self.config.get('redundancy_columns', [])
                        if redundancy_columns:
                            self.df = self.df.drop(*redundancy_columns)
#                             self.df.show()
                        else:
                            print("Error: Redundancy columns not specified in config.")
                    except Exception as e:
                        print(f"Error in data redundancy removal: {str(e)}")
            except Exception as e:
                print(f"Error in data_redundancy_removal: {str(e)}")
                
    
    # Showing final dataset
        def show_df(self):
            self.df.show()
            return self.df
    
    # save data to hdfs
        def get_dataframe(self):
            try:
                op_path = self.config["path"]["output_to_transform"]
                self.df.write.save(op_path)
                print("Successfully save data.")
                return self.df
            except Exception as e:
                print(f"Error in get_dataframe: {str(e)}")

if __name__ == "__main__":
    
    config_path = "/home/mavericbigdata12/Project_loan_default/config/config.json"
    # Instantiate DataCleanser class
    data_transform = DataTransform(config_path)

    # Load the dataset
    data_transform.load_dataset()
    
    # Funcitons 
    data_transform.data_aggregation()
    data_transform.data_filtering()
    data_transform.data_sorting()
    data_transform.data_joining_and_merging()   ## we dont have any other data set to join
    data_transform.data_splitting()
    data_transform.data_reshaping()
    data_transform.data_transposition()
    data_transform.data_imputation()
    data_transform.data_duplication()
    data_transform.data_extraction()
    data_transform.data_binning()
    data_transform.data_encoding()
    data_transform.data_normalization()
    data_transform.data_scaling()
    data_transform.data_agg_timeframes()
    data_transform.data_sampling()
    data_transform.data_parsing()
    data_transform.data_redundancy_removal()
    data_transform.data_transform_by_cal()
    data_transform.data_conversion()
    
    
    data_transform.show_df()
    Transformed_df = data_transform.get_dataframe()
    Data_Massaging.spark.stop()