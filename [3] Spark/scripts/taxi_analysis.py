from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import time

spark = SparkSession.builder \
    .appName("Bai2_NYCTaxi_DataFrame") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("Đang đọc dữ liệu và xử lý, vui lòng đợi vài giây...")
df = spark.read.csv("file:///opt/bitnami/spark/data/nyc_taxi_data.csv", header=True, inferSchema=True)

result_df = df.groupBy("payment_type") \
              .agg(avg("tip_amount").alias("avg_tip_amount")) \
              .orderBy("payment_type")

print("trung bình tiền tip theo loại hình thanh toán:")
result_df.show()

try:
    time.sleep(600)
except KeyboardInterrupt:
    pass

spark.stop()