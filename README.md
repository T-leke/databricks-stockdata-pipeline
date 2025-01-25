# **Financial Data Extraction and Transformation Pipeline using Databricks**

This project demonstrates a scalable data engineering solution for extracting, transforming, and storing financial data using the [Alpha Vantage API](https://www.alphavantage.co/). The pipeline is designed to extract historical stock data for multiple symbols, transform it into a structured format, and store it in a cloud-based data lake for downstream analytics.

---

## **Features**
- **Data Extraction**: Retrieves daily time-series stock data for given symbols using the Alpha Vantage API.
- **Data Transformation**: Cleanses and transforms raw JSON data into a structured format (e.g., date, open, high, low, close, volume).
- **Data Storage**: Saves the transformed data as `.csv` files to an Azure Data Lake for efficient querying and analysis.
- **Cloud Orchestration**: The pipeline is orchestrated and executed in Databricks Jobs.
- **Incremental Loading**: Supports loading new data incrementally to avoid duplicate processing.

---

## **Tech Stack**
- **Python**: Core language for extraction and transformation logic.
- **Databricks**: Cloud platform for executing the pipeline.
- **Azure Data Lake Storage (ADLS)**: Destination for storing transformed data.
- **Alpha Vantage API**: Source of financial data.
- **Pandas**: Data manipulation and transformation.
- **Git**: Version control and collaboration.

---

## **API Data Source**
The pipeline uses the **Alpha Vantage API** for financial market data. 

- **API Details**:
  - URL: `https://www.alphavantage.co/`
  - Function: `TIME_SERIES_DAILY`
  - Parameters: 
    - `symbol`: Stock ticker symbol (e.g., IBM, MSFT, GOOGL)
    - `apikey`: Your Alpha Vantage API key.

- **Rate Limits**: The free tier of Alpha Vantage limits API calls to **5 per minute** and **25 per day**. For higher limits, a [premium plan](https://www.alphavantage.co/premium/) is required.

---

## **Setup Instructions**
### **Prerequisites**
1. Python 3.x installed on your machine.
2. A Databricks workspace.
3. An Azure Data Lake Storage account.
4. Git installed and configured.

---

### **Steps to Run the Project**
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/your-repo.git
   cd your-repo
