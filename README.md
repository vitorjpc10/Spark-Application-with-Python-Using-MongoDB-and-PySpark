# Albion Online Market Data ETL Project (Spark, MongoDB)

## Project Overview

This project aims to create an ETL (Extract, Transform, Load) pipeline using PySpark to extract market data from the Albion Online Data API, transform it, and load it into MongoDB. The project consists of two main components: data extraction and data processing.

## Project Structure

- `albion_data_connector.py`: Contains the `AlbionDataFetcher` class for fetching data from the Albion Online Data API.
- `data_processor.py`: Utilizes PySpark to process the data, creating staging and mart data tables, and writes them to MongoDB.

## Prerequisites

- Python 3.8+
- PySpark 3.0.1+
- MongoDB 4.4+
- `requests` library for Python
- `pyspark` library for Python
- `org.mongodb.spark:mongo-spark-connector_2.12:3.0.1` for Spark

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/yourusername/albion-market-data-etl.git
    cd albion-market-data-etl
    ```

2. Install the required Python libraries:

    ```bash
    pip install requests pyspark
    ```

3. Ensure MongoDB is installed and running on `mongodb://127.0.0.1:27017`.

## Usage

1. **Fetch Data from Albion Online API**

   The `AlbionDataFetcher` class in `albion_data_connector.py` fetches market data from the Albion Online Data API.

    ```python
    import json
    import requests

    class AlbionDataFetcher:
        def __init__(self):
            self.url = (
                "https://albion-online-data.com/api/v2/stats/history/T7_MEAL_OMELETTE_AVALON,...?time-scale=24")

        def get_request(self):
            """Send a GET request to the URL, save the JSON response to a file, and return JSON object."""
            response = requests.get(self.url)
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"Failed to retrieve Albion Online Data: {response.status_code}")

    # Runtime test
    data_connector = AlbionDataFetcher()
    data = data_connector.get_request()
    print(json.dumps(data, indent=4))
    ```

2. **Process Data with PySpark**

   The `data_processor.py` script processes the data using PySpark, creating staging and mart data tables, and writes them to MongoDB.

    ```python
    import math
    from pyspark.sql import SparkSession
    from pyspark.sql import Row
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType
    from albion_data_connector import AlbionDataFetcher

    # Initialize Spark Session with MongoDB configurations
    spark = SparkSession.builder \
        .appName("mongo-spark") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/albion_data") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

    data_connector = AlbionDataFetcher()
    data = data_connector.get_request()

    def get_staging_data():
        # Convert JSON data to PySpark Rows
        row_data = []
        for item in data:
            for data_element in item.get("data", []):
                row_data.append(Row(
                    city_location=item.get("location"),
                    item_id=item.get("item_id"),
                    item_quality=item.get("quality"),
                    item_count=data_element.get("item_count"),
                    avg_price=data_element.get("avg_price"),
                    timestamp=data_element.get("timestamp")
                ))
        return row_data

    def get_mart_data():
        # Convert JSON data to PySpark Rows
        row_data = []
        for item in data:
            city_location = item.get("location")
            item_id = item.get("item_id")
            item_quality = item.get("quality")

            if len(item.get("data")) == 1:
                average_price = float(data_element.get("avg_price"))
                average_item_count = float(data_element.get("item_count"))
                last_days_count = 1
            else:
                average_price = 0
                average_item_count = 0
                last_days_count = 0

                for data_element in item.get("data", [])[1:8]:  # First 7 full days if available
                    average_price += data_element.get("avg_price")
                    average_item_count += data_element.get("item_count")
                    last_days_count += 1

                average_price = average_price / last_days_count
                average_item_count = average_item_count / last_days_count

            row_data.append(Row(
                city_location=city_location,
                item_id=item_id,
                item_quality=item_quality,
                average_price=average_price,
                average_item_count=average_item_count,
                last_days_count=last_days_count
            ))

        return row_data

    if data:
        # Gets all staging data from API data
        staging_data = get_staging_data()

        # Create DataFrame from staging data
        df = spark.createDataFrame(staging_data)
        print("STAGING DATA:")
        df.show(truncate=False)
        df.printSchema()

        # Write Staging Data DataFrame to MongoDB
        df.write.format("mongo").mode("overwrite").option("database", "albion_data_price_history").option("collection",
                                                                                                       "staging_data").save()

        # Gets all mart data from API data with transformations
        mart_data = get_mart_data()

        # Define the schema for the DataFrame
        mart_schema = StructType([
            StructField("city_location", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("item_quality", StringType(), True),
            StructField("average_price", FloatType(), True),  # Ensure average_price is FloatType
            StructField("average_item_count", FloatType(), True),  # Assuming this should also be FloatType
            StructField("last_days_count", IntegerType(), True),  # Assuming this should also be IntegerType
        ])

        # Create DataFrame from mart data
        df = spark.createDataFrame(mart_data, schema=mart_schema)
        print("MART DATA:")
        df.show(truncate=False)
        df.printSchema()

        # Write Mart Data DataFrame to MongoDB
        df.write.format("mongo").mode("overwrite").option("database", "albion_data_price_history").option("collection",
                                                                                                       "mart_data").save()
    ```

## Albion Online Data API

These tools are not affiliated with Albion Online or Sandbox Interactive GmbH. The goal of this project is to collect and distribute real-time information for Albion Online. This is achieved with a downloadable client that monitors network traffic specifically for Albion Online, identifies the relevant information, and then ships it off to a central server which distributes the information to anyone who wants it.

For more details, visit the [Albion Online Data Project](https://albion-online-data.com/).


## Table Previews on MongoDB Compass

### Staging:
![img.png](img.png)

### Mart: 
![img_1.png](img_1.png)