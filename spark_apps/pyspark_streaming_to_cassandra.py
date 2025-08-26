from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType

# Schema cho dữ liệu JSON từ Kafka
schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("temperature", FloatType(), False),
    StructField("timestamp", DoubleType(), False)
])

# Hàm ghi một batch vào Cassandra
def write_to_cassandra(df, epoch_id):
    df.withColumn("timestamp", col("timestamp").cast("timestamp")) \
      .withColumn("date", to_date(col("timestamp"))) \
      .write \
      .format("org.apache.spark.sql.cassandra") \
      .options(keyspace="iot_platform", table="raw_sensor_data") \
      .mode("append") \
      .save()
    print(f"Batch {epoch_id} written to Cassandra.")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("IoT_Stream_Processor") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # Đọc luồng từ Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "iot-telemetry") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON và bắt đầu ghi
    df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    query = df_parsed.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("update") \
        .start()

    query.awaitTermination()