import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.1,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.720,org.postgresql:postgresql:42.6.0 default'
# Khởi tạo Spark session (Iceberg, MinIO, PostgreSQL)
spark = SparkSession.builder \
    .appName("SIEM-Cold-Storage-Test") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.siem_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.siem_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
    .config("spark.sql.catalog.siem_catalog.uri", "jdbc:postgresql://localhost:5432/iceberg_catalog") \
    .config("spark.sql.catalog.siem_catalog.jdbc.user", "iceberg") \
    .config("spark.sql.catalog.siem_catalog.jdbc.password", "iceberg") \
    .config("spark.sql.catalog.siem_catalog.warehouse", "s3a://siem-cold-storage/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Khởi tạo kết nối thành công! Đang tạo mock data...")

# Tạo mock data mô phỏng BOTS dataset
data = [
    (datetime.now(), "admin", "192.168.1.100", "failed_login"),
    (datetime.now(), "root", "10.0.0.55", "failed_login"),
    (datetime.now(), "user_01", "172.16.0.4", "lateral_movement")
]

schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("user", StringType(), True),
    StructField("src_ip", StringType(), True),
    StructField("event_type", StringType(), True)
])

df = spark.createDataFrame(data, schema)

# Tạo database và bảng Iceberg, sau đó ghi dữ liệu
spark.sql("CREATE NAMESPACE IF NOT EXISTS siem_catalog.logs")

print("Đang ghi log vào Iceberg (MinIO)...")
# append data vào bảng auth_logs
df.writeTo("siem_catalog.logs.auth_logs") \
    .tableProperty("format-version", "2") \
    .create() 

print("Ghi thành công! Đọc lại dữ liệu từ Iceberg:")
# Đọc lại dữ liệu
spark.table("siem_catalog.logs.auth_logs").show()

spark.stop()