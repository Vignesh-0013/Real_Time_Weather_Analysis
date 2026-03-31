from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, max, min, when, window
from pyspark.sql.types import StructType, StringType, DoubleType
from pymongo import MongoClient

spark = SparkSession.builder \
    .appName("AdvancedWeatherAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("wind_speed", DoubleType()) \
    .add("rainfall", DoubleType()) \
    .add("pressure", DoubleType()) \
    .add("timestamp", DoubleType())

# 🔹 Read Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather-topic") \
    .option("startingOffsets", "latest") \
    .load()

# 🔹 Parse JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 🔹 Convert timestamp
events = json_df.withColumn("event_time", col("timestamp").cast("timestamp"))

# 🔹 Analytics (AVG)
analytics = events.groupBy(
    window(col("event_time"), "60 seconds"),
    col("city")
).agg(
    avg("temperature").alias("avg_temp"),
    avg("humidity").alias("avg_humidity"),
    avg("wind_speed").alias("avg_wind"),
    avg("rainfall").alias("avg_rain"),
    avg("pressure").alias("avg_pressure"),
    max("temperature").alias("max_temp"),
    min("temperature").alias("min_temp")
)

# 🔹 Alerts
analytics = analytics \
    .withColumn("temp_alert", when(col("avg_temp") > 40, "HIGH").otherwise("NORMAL")) \
    .withColumn("humidity_alert", when(col("avg_humidity") > 80, "HIGH").otherwise("NORMAL")) \
    .withColumn("wind_alert", when(col("avg_wind") > 25, "HIGH").otherwise("NORMAL")) \
    .withColumn("rain_alert", when(col("avg_rain") > 15, "HIGH").otherwise("NORMAL")) \
    .withColumn("pressure_alert",
                when((col("avg_pressure") < 990) | (col("avg_pressure") > 1035), "HIGH")
                .otherwise("NORMAL"))

# 🔹 Write analytics to Mongo
def write_analytics_to_mongo(batch_df, epoch_id):
    rows = batch_df.collect()
    if rows:
        client = MongoClient("mongodb://mongo:27017/")
        db = client["weather_db"]
        coln = db["analytics"]

        data = []
        for r in rows:
            data.append({
                "city": r["city"],
                "window_start": str(r["window"].start),
                "window_end": str(r["window"].end),
                "avg_temp": float(r["avg_temp"]),
                "avg_humidity": float(r["avg_humidity"]),
                "avg_wind": float(r["avg_wind"]),
                "avg_rain": float(r["avg_rain"]),
                "avg_pressure": float(r["avg_pressure"]),
                "max_temp": float(r["max_temp"]),
                "min_temp": float(r["min_temp"]),
                "temp_alert": r["temp_alert"],
                "humidity_alert": r["humidity_alert"],
                "wind_alert": r["wind_alert"],
                "rain_alert": r["rain_alert"],
                "pressure_alert": r["pressure_alert"]
            })

        coln.delete_many({})
        coln.insert_many(data)

# 🔹 Write LIVE data (latest values)
def write_live_to_mongo(batch_df, epoch_id):
    rows = batch_df.collect()
    if rows:
        client = MongoClient("mongodb://mongo:27017/")
        db = client["weather_db"]
        live_col = db["live_data"]

        for r in rows:
            record = {
                "city": r["city"],
                "temperature": float(r["temperature"]),
                "humidity": float(r["humidity"]),
                "wind_speed": float(r["wind_speed"]),
                "rainfall": float(r["rainfall"]),
                "pressure": float(r["pressure"])
            }

            live_col.update_one(
                {"city": record["city"]},
                {"$set": record},
                upsert=True
            )

# 🔹 Start streams
query1 = analytics.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_analytics_to_mongo) \
    .start()

query2 = json_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_live_to_mongo) \
    .start()

spark.streams.awaitAnyTermination()