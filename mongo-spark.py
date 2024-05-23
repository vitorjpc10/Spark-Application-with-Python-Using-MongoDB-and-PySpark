from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
import json
from sample_data.albion_data_connector import AlbionDataFetcher

data_connector = AlbionDataFetcher()
data = data_connector.get_request()

# print(json.dumps(data, indent=4))

# Initialize Spark Session with MongoDB configurations
spark = SparkSession.builder \
    .appName("mongo-spark") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/albion_data") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

if data:
    # Convert JSON data to PySpark Rows
    row_data = []
    for item in data:
        city_location = item.get("location")
        item_id = item.get("item_id")
        item_quality = item.get("quality")
        for entry in item.get("data", []):
            row_data.append(Row(
                city_location=city_location,
                item_id=item_id,
                item_quality=item_quality,
                item_count=entry.get("item_count"),
                avg_price=entry.get("avg_price"),
                timestamp=entry.get("timestamp")
            ))

    # Create DataFrame from Rows
    df = spark.createDataFrame(row_data)
    df.show(truncate=False)
    df.printSchema()

    # Write DataFrame to MongoDB
    df.write.format("mongo").mode("append").option("database", "albion_data").option("collection", "items").save()



