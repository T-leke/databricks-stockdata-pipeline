import pandas as pd
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
import logging

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

file_path ='abfss://capital-container@newcapitaledgestorage.dfs.core.windows.net/'

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set logging level (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    handlers=[
        logging.FileHandler("data_transformation.log"),  # Log to a file
        logging.StreamHandler()  # Log to console
    ]
)

# this function takes the extracted data as imput and transform using the date_dim created
def transform_data(data):

    try:
        logging.info("Starting data transformation process.")
        # Creating the date dimension
        start_date = datetime(2020, 1, 1)
        current_date = datetime.now()

        # Calculate a list of dates from start date to current date
        num_days = (current_date - start_date).days
        date_list = [start_date + timedelta(days=x) for x in range(num_days + 1)]

        # Ensure date_id matches the length of the date list
        date = {'date_id': [x for x in range(1, len(date_list) + 1)], 'date': date_list}

        date_dim = pd.DataFrame(date)
        date_dim['Year'] = date_dim['date'].dt.year
        date_dim['Month'] = date_dim['date'].dt.month
        date_dim['Day'] = date_dim['date'].dt.day
        date_dim['date'] = pd.to_datetime(date_dim['date']).dt.date
        
        # Define output path in DBFS
        output_path = f"{file_path}/date_dim.csv"

        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(date_dim)
        # Write to Azure Data Lake in CSV format
        spark_df.write.csv(output_path, header=True, mode="overwrite")
        logging.info(f"Date dimension file saved successfully to {output_path}.")

        data['date'] = pd.to_datetime(data['date']).dt.date
        if data['date'].isnull().any():
            raise ValueError("Invalid dates found in 'date' column after conversion.")

        # Merging and transforming data
        dim = data.merge(date_dim, left_on='date', right_on='date', how='inner')\
                    .rename(columns={'date_id': 'StockDate_ID'})\
                    .reset_index(drop=True)\
                    [['StockDate_ID', 'open', 'high', 'low', 'close', 'volume']] 

        logging.info("Data transformed successfully.")
        return dim
    
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    return None


