import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lit, to_date

def run_etl(report_date):
    spark = SparkSession.builder \
        .appName("ETL_Cassandra_to_Postgres") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.read.timeout_ms", "300000") \
        .config("spark.cassandra.connection.timeout_ms", "30000") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    print(f"Running ETL for date: {report_date}")
    
    # 1. Extract từ Cassandra - Tối ưu hóa để tránh timeout
    print(f"Running ETL for date: {report_date}")
    
    try:
        # Đọc dữ liệu với filter ngay từ đầu để giảm tải
        df_raw = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(keyspace="iot_platform", table="raw_sensor_data") \
            .load() \
            .filter(col("date").cast("string") == report_date)
        
        # Kiểm tra số lượng records
        record_count = df_raw.count()
        print(f"Found {record_count} records for date {report_date}")
        
        if record_count == 0:
            # Thử cách khác nếu không tìm thấy dữ liệu
            print("Trying alternative date format...")
            df_raw = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(keyspace="iot_platform", table="raw_sensor_data") \
                .load() \
                .filter(col("date") == to_date(lit(report_date)))
            
            record_count = df_raw.count()
            print(f"Found {record_count} records with date type comparison")
            
    except Exception as e:
        print(f"Error reading from Cassandra: {str(e)}")
        print("Trying to read with smaller partitions...")
        
        # Fallback: đọc với cấu hình partition nhỏ hơn
        df_raw = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(keyspace="iot_platform", table="raw_sensor_data") \
            .option("spark.cassandra.input.split.size_in_mb", "64") \
            .load() \
            .filter(col("date").cast("string") == report_date)
        
        record_count = df_raw.count()
        print(f"Found {record_count} records with smaller partitions")

    if df_raw.rdd.isEmpty():
        print(f"No data found for {report_date}. Exiting.")
        return

    # 2. Transform: Tính nhiệt độ trung bình với tối ưu hóa
    print("Starting data transformation...")
    
    # Cache dữ liệu để tránh đọc lại từ Cassandra
    df_raw.cache()
    
    df_agg = df_raw.groupBy("sensor_id") \
                   .agg(avg("temperature").alias("avg_temperature")) \
                   .withColumn("report_date", to_date(lit(report_date)))
    
    # Coalesce để giảm số partition trước khi ghi
    df_agg = df_agg.coalesce(1)

    print(f"Aggregated data for {report_date}:")
    df_agg.show()
    
    agg_count = df_agg.count()
    print(f"Total aggregated records: {agg_count}")

    # 3. Load vào PostgreSQL với tối ưu hóa
    if agg_count > 0:
        print("Writing to PostgreSQL...")
        df_agg.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres_analytics:5432/analytics_db") \
            .option("dbtable", "public.daily_avg_temperature") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", "1000") \
            .option("isolationLevel", "READ_UNCOMMITTED") \
            .mode("append") \
            .save()
        print(f"Successfully loaded {agg_count} records for {report_date} to PostgreSQL.")
    else:
        print("No aggregated data to write to PostgreSQL.")
    
    # Cleanup
    df_raw.unpersist()
    print("ETL process completed.")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: etl_cassandra_to_postgres.py <YYYY-MM-DD>")
        sys.exit(1)
    
    report_date_arg = sys.argv[1]
    run_etl(report_date_arg)