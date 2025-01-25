import pandas as pd
import os
import requests
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set logging level (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    handlers=[
        logging.FileHandler("data_extraction.log"),  # Log to a file
        logging.StreamHandler()  # Log to console
    ]
)

def extract_data(symbol):

    try:
        #Load the environment variables from the .env files

       
        apikey = ''

        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey=Y91Y4K4QPYCHMU8U'
        #send a request to the API
        logging.info(f"Sending request to API for symbol: {symbol}")
        r = requests.get(url)

        if r.status_code != 200:
            raise Exception(f"Error: Received status code {r.status_code} from the API.")
        
        #Parse the API response
        logging.info(f"Parsing API response for symbol: {symbol}")
        data = r.json()
        print(data)

        #creating a dataframe from the json extracted
        frame = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient='index').reset_index()
        frame.rename(columns={
            'index' : 'date',
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. volume': 'volume'
        }, inplace= True
        )

        logging.info("Data extracted successfully")
        return frame
    
    except Exception as e:
        logging.error(f"Error during data extraction: {str(e)}")
        return None