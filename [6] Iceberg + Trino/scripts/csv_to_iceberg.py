from pyspark.sql import SparkSession
import sys

def main():
    print("--- KHỞI TẠO SPARK SESSION ---")
    spark = SparkSession.builder \
        .appName("CSV-to-Iceberg") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "rest") \
        .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
        .config("spark.sql.catalog.demo.s3.access-key-id", "admin") \
        .config("spark.sql.catalog.demo.s3.secret-access-key", "password") \
        .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/") \
        .config("spark.sql.defaultCatalog", "demo") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("✓ Spark Session đã khởi tạo thành công với Iceberg REST Catalog!\n")

    print("--- BƯỚC 1: ĐỌC DỮ LIỆU CSV TỪ MINIO ---")
    csv_path = "s3a://rawdata/csv/nyc_taxi_data.csv"
    
    try:
        df_csv = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .csv(csv_path)
        print("✓ Đọc CSV thành công. Schema của dữ liệu:")
        df_csv.printSchema()
        print(f"Tổng số dòng trong CSV: {df_csv.count()}\n")
    except Exception as e:
        print(f"❌ Lỗi khi đọc CSV: {e}")
        print("Vui lòng kiểm tra xem container 'mc' đã upload file csv lên MinIO chưa.")
        sys.exit(1)

    print("--- BƯỚC 2: GHI DỮ LIỆU SANG ICEBERG TABLE ---")
    print("Đang xử lý và ghi dữ liệu... (quá trình này có thể mất 1-2 phút tùy cấu hình máy)")
    
    # Tạo namespace nyc
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.nyc")
    
    # Ghi dữ liệu vào bảng Iceberg
    df_csv.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("demo.nyc.taxis")
        
    print("✓ Đã ghi dữ liệu thành công vào bảng Iceberg: demo.nyc.taxis\n")

    print("--- BƯỚC 3: KIỂM TRA LẠI DỮ LIỆU ICEBERG ---")
    count_df = spark.sql("SELECT COUNT(*) AS total FROM demo.nyc.taxis")
    print(f"Tổng số dòng trong bảng Iceberg: {count_df.first().total}")
    
    print("\n--- HOÀN TẤT ---")
    print("Bây giờ bạn có thể mở Trino để truy vấn và so sánh tốc độ!")
    
    spark.stop()

if __name__ == "__main__":
    main()
