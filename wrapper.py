#its gives all informtion about our dataset 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("wrapper").getOrCreate()
import sys
import os


def analyze_dataframe(df, data_path):

    #File size
    spark = SparkSession.builder \
        .appName("File Size") \
        .getOrCreate()
    
    print("="*100)
    

#     data_path = r"C:\Users\sahill\Desktop\LoanDefault\Loan_Default_Project\data\processed\Loan_Default.csv"
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(data_path)
    file_status = fs.getFileStatus(path)
    print(file_status)
    file_size_bytes = file_status.getLen()
    file_size_kb = file_size_bytes / 1024

    print(f"File size in kilobytes: {file_size_kb}")
    
    print("="*100)


    # Get the number of rows
    row_count = df.count()
    print("\nNumber of rows:", row_count)
    
    print("="*100)

    # Get the number of columns
    column_count = len(df.columns)
    print("Number of columns:", column_count)
    print("="*100)


    # Get column names
    column_names = df.columns
    for i in range(len(column_names)):
        if (i+1)%5 == 0:
            print(column_names[i], end= "\n")
        else:
            print(column_names[i], end= "\t|")
    print()
    print("="*100)

    # Get distinct count in each column
    print("Distinct counts for each column:")
    for col in df.columns:
        count=df.select(col).distinct().count()
        print(f"{col}: {count}")
    print("="*100)


    # Get data types for each column
    data_types = {col: df.schema[col].dataType for col in df.columns}
    print("\nData types:")
    for col, dtype in data_types.items():
        print(f"{col}: {dtype}")
    print("="*100)
    
    # Get null values count for each column
    null_counts = {col: df.filter(df[col].isNull()).count() for col in df.columns}
    print("\nNull values count:")
    for col, count in null_counts.items():
        print(f"{col}: {count}")
    print("="*100)
    
    # Get null values in %
    null_counts = {col: df.filter(df[col].isNull()).count() for col in df.columns}
    print("\n %Null values count:")
    for col, count in null_counts.items():
        print(f"{col}: {(count/df.count())*100}")
    print("="*100)

    # Get duplicate rows count
    duplicate_count = df.groupBy(df.columns).count().where('count > 1').count()
    print("\nDuplicate rows count:", duplicate_count)
    print("="*100)
    df.show(10)


def data_ingestion(data_path, file_format='csv', header=True,truncate=False, infer_schema=True, options=None):
    spark = SparkSession.builder \
        .appName("Data Ingestion") \
        .getOrCreate()

    try:
        if file_format == 'csv':

            df = spark.read.csv(data_path, header=True, schema=schema, **options)
        elif file_format == 'parquet':
            df = spark.read.parquet(data_path, **options)



        analyze_dataframe(df,data_path)



        # df.write.format("jdbc").options(url="jdbc:mysql://_host:port/database_name",
        #                                  driver="com.mysql.jdbc.Driver",
        #                                  dbtable="table_name",
        #                                  user="username",
        #                                  password="password").mode("overwrite").save()

    except Exception as e:
        print("Error occurred during data ingestion:", str(e))
    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    import json
    config_path = "/home/mavericbigdata12/Project_loan_default/config/config.json"
    # Read configuration from JSON file
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
        
    data_path = config["path"]["data_file_path"]
    #data_path = sys.argv[1]
#     data_path = "file:///home/mavericbigdata12/Project_loan_default/Watcher_Script/Watcher/Loan_Default.csv"
    options = {"sep": ",", "nullValue": "NA"}


    data_ingestion(data_path, file_format='csv', options=options)