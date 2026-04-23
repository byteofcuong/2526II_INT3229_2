from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window, count, sum, expr

conf = SparkConf()
spark = SparkSession.builder.config(conf=conf).appName("TaxiAnalyticsBatch").getOrCreate()

def load_config(spark_context):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    spark_context._jsc.hadoopConfiguration().set("fs.defaultFS", "s3://warehouse-v")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio.kafka.svc.cluster.local:9000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")

load_config(spark.sparkContext)

print("Đang tải dữ liệu từ Parquet (Kho chứa Streaming gốc)...")
try:
    # 1. Đọc dữ liệu từ thư mục Parquet
    raw_df = spark.read.parquet("s3a://warehouse-v/k8/spark-stream/")

    # Tiền xử lý: Ép kiểu chuỗi thời gian về định dạng Timestamp chuẩn để hỗ trợ Window
    processed_df = raw_df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
                         .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))

    # 2. Xóa các bản ghi rác: Quãng đường hoặc số lượng khách bị âm
    print("Thực hiện làm sạch dữ liệu (Data Cleaning)...")
    clean_df = processed_df.filter((col("trip_distance") >= 0) & (col("passenger_count") >= 0))

    # 3. Tạo cột mới: is_high_tips dựa trên điều kiện tips > 20% tiền cước
    print("Gắn nhãn: is_high_tips...")
    enhanced_df = clean_df.withColumn("is_high_tips", col("tip_amount") > (col("fare_amount") * 0.2))

    # 4. Gom nhóm theo mốc thời gian (Window 10 phút) tính tổng lượng và doanh thu
    print("Gom nhóm dữ liệu dạng 10 phút (Window Aggregations)...")
    windowed_df = enhanced_df.groupBy(window(col("tpep_pickup_datetime"), "10 minutes")) \
        .agg(
            count("*").alias("total_trips"),
            sum("total_amount").alias("total_revenue")
        )

    # Nếu bạn muốn test trực trập trên màn hình (terminal) bạn có thể giữ lệnh show()
    # windowed_df.show(truncate=False)

    # 5. Lưu kết xuất dữ liệu phân tích ra thư mục mới (Silver/Gold layer)
    output_path = "s3a://warehouse-v/k8/spark-analytics/"
    print(f"Lưu kết quả tổng hợp 10 phút xuống Data Lake (Parquet): {output_path}")
    
    windowed_df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print("Hoàn tất xử lý Batch Analytics!")

except Exception as e:
    print(f"Chưa có dữ liệu Parquet để chạy hoặc lỗi môi trường: {e}")

spark.stop()
