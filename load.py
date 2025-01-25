# Databricks notebook source
import pandas as pd
from transformation import transform_data
from extract import extract_data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("incremental_loading.log"),  # Log to a file
        logging.StreamHandler()  # Log to console
    ]
)

# Extract the data using extract.py
logging.info("Extracting data for IBM, Microsoft, and Google.")
IBM_df = extract_data('IBM')
MICROSOFT_df = extract_data('MSFT')
GOOGLE_df = extract_data('GOOGL')

# Transform the extracted data using transform.py
logging.info("Transforming data for IBM, Microsoft, and Google.")
IBM_dim = transform_data(IBM_df)
MICROSOFT_dim = transform_data(MICROSOFT_df)
GOOGLE_dim = transform_data(GOOGLE_df)

# File paths in Azure Blob Storage
file_path = 'abfss://capital-container@newcapitaledgestorage.dfs.core.windows.net'
IBM_file_path = f"{file_path}/IBM_dim.csv"
GOOGLE_file_path = f"{file_path}/GOOGLE_dim.csv"
MSFT_file_path = f"{file_path}/MSFT_dim.csv"


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def incremental_loading(file_path, latest_stock_data):
    """
    Function to perform incremental loading of data to Azure Blob Storage using Spark.

    Args:
        file_path (str): The full path to the file in Azure Blob Storage.
        latest_stock_data (DataFrame): The latest stock data to be incrementally loaded.
    """
    try:
        # Convert pandas DataFrame to Spark DataFrame
        logging.info(f"Starting incremental loading for {file_path}.")
        latest_spark_df = spark.createDataFrame(latest_stock_data)

        # Check if the file exists in Azure Blob Storage
        file_exists = False
        try:
            dbutils.fs.ls(file_path)
            file_exists = True
        except Exception as e:
            # File does not exist
            file_exists = False

        if not file_exists:
            # If file doesn't exist, save the latest stock data
            logging.info(f"File does not exist. Saving new data to {file_path}.")
            latest_spark_df.write.csv(file_path, header=True, mode="overwrite")
            logging.info(f"File saved successfully to {file_path}.")
        else:
            logging.info(f"File {file_path} already exists in Azure Blob Storage. Fetching existing data.")

            # Read the existing data from Azure Blob Storage into a Spark DataFrame
            existing_spark_df = spark.read.csv(file_path, header=True, inferSchema=True)

            # Ensure both DataFrames have the same schema
            existing_spark_df = existing_spark_df.select(*latest_spark_df.columns)

            # Fetch the last recorded StockDate_ID from the existing data
            existing_max_dateID = existing_spark_df.selectExpr("MAX(StockDate_ID) as StockDate_ID").collect()[0]["StockDate_ID"]

            # Fetch the latest StockDate_ID from the new data
            latest_min_dateID = latest_stock_data["StockDate_ID"].min()

            # Check if an update is required
            if latest_min_dateID > existing_max_dateID:
                logging.info("New data detected. Updating the existing file.")

                # Filter only the new rows from the latest data
                new_data_spark_df = latest_spark_df.filter(col("StockDate_ID") > existing_max_dateID)

                # Union the existing data with the new data
                updated_spark_df = new_data_spark_df.union(existing_spark_df)

                # Overwrite the existing file with the updated data
                updated_spark_df.write.csv(file_path, header=True, mode="overwrite")
                logging.info(f"File {file_path} has been updated in Azure Blob Storage.")
            else:
                logging.info("No updates required. The record is up-to-date.")
    except Exception as e:
        logging.error(f"An unexpected error occurred while performing incremental loading: {e}")

# Perform incremental loading for each stock
incremental_loading(IBM_file_path, IBM_dim)
incremental_loading(GOOGLE_file_path, GOOGLE_dim)
incremental_loading(MSFT_file_path, MICROSOFT_dim)
