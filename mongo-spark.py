import math

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType

from albion_sample_data.albion_data_connector import AlbionDataFetcher

# Initialize Spark Session with MongoDB configurations
spark = SparkSession.builder \
    .appName("mongo-spark") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/albion_data") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()


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

data_connector = AlbionDataFetcher()
data = data_connector.get_request()

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
        StructField("average_price", FloatType(), True),  # Ensure average_price is DoubleType
        StructField("average_item_count", FloatType(), True),  # Assuming this should also be DoubleType
        StructField("last_days_count", IntegerType(), True), #  # Assuming this should also be DoubleType
])

    # Create DataFrame from staging data
    df = spark.createDataFrame(mart_data, schema=mart_schema)
    print("MART DATA:")
    df.show(truncate=False)
    df.printSchema()

    # Write Staging Data DataFrame to MongoDB
    df.write.format("mongo").mode("overwrite").option("database", "albion_data_price_history").option("collection",
                                                                                                   "mart_data").save()
