# Lab 6: Iceberg + Trino - Hướng dẫn chi tiết & So sánh tốc độ

Lab này cung cấp cho bạn một môi trường Data Lakehouse hoàn chỉnh chạy trên Docker cục bộ, bao gồm việc lưu trữ dữ liệu với **MinIO**, chuyển đổi và quản lý bảng với **Apache Spark & Iceberg**, và truy vấn phân tích siêu tốc với **Trino**.

Mục tiêu chính là giúp bạn hiểu quy trình tự động đọc file `csv`, chuyển thành định dạng `Iceberg table` qua một script PySpark, và cuối cùng so sánh tốc độ truy vấn trên 2 định dạng này thông qua Trino.

---

## 🏗 Kiến trúc Hệ thống

- **MinIO**: Đóng vai trò là Amazon S3 cục bộ (Lớp lưu trữ / Storage Layer).
- **Iceberg REST Catalog**: Quản lý metadata của các bảng Iceberg.
- **Spark-Iceberg**: Compute engine chạy script Python để đọc file CSV thô và ghi thành chuẩn Iceberg.
- **Hive Metastore**: Quản lý metadata cho connector Hive của Trino (để Trino hiểu được schema của file CSV).
- **Trino**: Truy vấn dữ liệu đồng thời từ cả CSV (qua Hive) và Iceberg.

---

## 🚀 Hướng Dẫn Thực Hành Chi Tiết

### Bước 1: Khởi động hệ thống Docker

1. Mở Terminal / Command Prompt tại thư mục bài lab này (`[6] Iceberg + Trino`).
2. Chạy lệnh sau để tải và khởi động toàn bộ kiến trúc (gồm 6 container):
   ```bash
   docker-compose up -d --remove-orphans
   ```
   > **Lưu ý:** Quá trình khởi tạo lần đầu có thể mất từ 5-10 phút để tải các Docker image. 
   Sau khi hoàn tất, hãy đợi khoảng 1-2 phút để tất cả các dịch vụ (đặc biệt là Hive Metastore và Trino) khởi động hoàn toàn.

3. **Chuyện gì đang xảy ra ngầm?**
   - Container `mc` (MinIO Client) đang tự động tạo bucket `warehouse` (dùng cho Iceberg) và `rawdata` (dùng cho CSV gốc).
   - Container `mc` cũng tự động copy file `nyc_taxi_data.csv` (khoảng 610,000 dòng) vào bucket `rawdata` trên MinIO.
   - Bạn có thể tự mình kiểm chứng bằng cách truy cập MinIO Console tại: [http://localhost:9001](http://localhost:9001) 
     - **Tài khoản:** `admin`
     - **Mật khẩu:** `password`

---

### Bước 2: Chuyển CSV sang định dạng Iceberg (Bằng Script)

Trong thư mục `scripts/`, chúng ta đã chuẩn bị sẵn file `csv_to_iceberg.py`. File này chứa các lệnh PySpark để đọc file CSV từ bucket `rawdata` và ghi thành bảng Iceberg vào bucket `warehouse`.

Thực thi kịch bản này bằng cách chạy lệnh sau trên Terminal của máy bạn:

```bash
docker exec -it spark-iceberg spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /home/iceberg/scripts/csv_to_iceberg.py
```

**Kết quả mong đợi:** 
Bạn sẽ thấy terminal in ra quá trình khởi tạo Spark Session, số lượng dòng đọc được từ CSV (~610k dòng), và thông báo ghi dữ liệu thành công vào bảng Iceberg `demo.nyc.taxis`. Toàn bộ quá trình chạy có thể mất từ 1-2 phút tùy tốc độ máy tính của bạn.

Sau khi chạy xong, nếu vào MinIO (bucket `warehouse`), bạn sẽ thấy cấu trúc thư mục chứa các file metadata `.json`, `.avro` và file dữ liệu `.parquet` của Iceberg.

---

### Bước 3: Truy cập Trino và Cấu hình bảng CSV

Chúng ta sẽ dùng Trino CLI (được tích hợp sẵn trong container) để truy cập vào Trino server đang chạy.

1. Mở một terminal mới (hoặc dùng terminal hiện tại) và gõ:
   ```bash
   docker exec -it trino trino
   ```
   *Màn hình terminal của bạn sẽ chuyển sang giao diện gõ lệnh của Trino (`trino>`).*

2. **Khai báo bảng CSV cho Trino (qua Hive Connector):**
   Mặc dù dữ liệu CSV đã nằm trên MinIO, Trino chưa biết cấu trúc của nó. Chúng ta sẽ dùng Hive Metastore để báo cho Trino biết. Cập nhật schema bằng cách copy toàn bộ đoạn mã SQL dưới đây và dán vào cửa sổ Trino:

   ```sql
   CREATE SCHEMA IF NOT EXISTS hive.default;

   CREATE TABLE IF NOT EXISTS hive.default.taxis_csv (
     VendorID VARCHAR,
     tpep_pickup_datetime VARCHAR,
     tpep_dropoff_datetime VARCHAR,
     passenger_count VARCHAR,
     trip_distance VARCHAR,
     pickup_longitude VARCHAR,
     pickup_latitude VARCHAR,
     RatecodeID VARCHAR,
     store_and_fwd_flag VARCHAR,
     dropoff_longitude VARCHAR,
     dropoff_latitude VARCHAR,
     payment_type VARCHAR,
     fare_amount VARCHAR,
     extra VARCHAR,
     mta_tax VARCHAR,
     tip_amount VARCHAR,
     tolls_amount VARCHAR,
     improvement_surcharge VARCHAR,
     total_amount VARCHAR
   ) WITH (
     format = 'CSV',
     external_location = 's3a://rawdata/csv/',
     skip_header_line_count = 1
   );
   ```

3. **Kiểm tra các bảng hiện có:**
   Để chắc chắn Trino đã nhận dạng được cả hai bảng, gõ các lệnh sau:
   ```sql
   -- Hiện bảng từ Iceberg
   SHOW TABLES IN iceberg.nyc;

   -- Hiện bảng từ CSV (Hive)
   SHOW TABLES IN hive.default;
   ```

---

### Bước 4: So sánh tốc độ truy xuất (Benchmarking)

Đây là bước quan trọng nhất của Lab. Hãy chạy từng cặp câu lệnh sau trên Trino và quan sát **thời gian thực thi (Execution time)** hiển thị ở cuối mỗi kết quả trả về.

#### Bài test 1: Đếm tổng số dòng (Count All)
```sql
-- Dữ liệu CSV thô (Row-based)
SELECT count(*) FROM hive.default.taxis_csv;

-- Dữ liệu Iceberg (Metadata/Manifest)
SELECT count(*) FROM iceberg.nyc.taxis;
```
> 💡 **Giải thích:** Iceberg sẽ phản hồi gần như lập tức (vài mili-giây). Trino chỉ cần đọc file manifest (metadata) của Iceberg để lấy tổng số record mà không cần quyét qua nội dung file thực tế. Ngược lại, với CSV, engine bắt buộc phải scan toàn bộ file từ đầu đến cuối để đếm.

#### Bài test 2: Gom nhóm & Tính toán (Aggregation)
```sql
-- Dữ liệu CSV thô
SELECT payment_type, SUM(CAST(fare_amount AS DOUBLE)) 
FROM hive.default.taxis_csv 
GROUP BY payment_type;

-- Dữ liệu Iceberg
SELECT payment_type, SUM(fare_amount) 
FROM iceberg.nyc.taxis 
GROUP BY payment_type;
```
> 💡 **Giải thích:** Iceberg lưu trữ dữ liệu dưới định dạng Parquet (Column-based). Khi truy vấn cột `payment_type` và `fare_amount`, hệ thống sẽ chỉ quét đúng 2 cột đó, bỏ qua 17 cột còn lại. Với CSV (Row-based), toàn bộ các hàng trong file phải được load từ đĩa vào bộ nhớ, tốn rất nhiều I/O và RAM.

#### Bài test 3: Tìm kiếm có điều kiện (Filtering / Data Pruning)
```sql
-- Dữ liệu CSV thô
SELECT * FROM hive.default.taxis_csv 
WHERE CAST(trip_distance AS DOUBLE) > 10 
LIMIT 10;

-- Dữ liệu Iceberg
SELECT * FROM iceberg.nyc.taxis 
WHERE trip_distance > 10 
LIMIT 10;
```
> 💡 **Giải thích:** Iceberg duy trì giá trị Min/Max của từng cột ở cấp độ file/chunk (trong metadata). Khi có điều kiện `trip_distance > 10`, Iceberg có thể loại bỏ hoàn toàn các chunk dữ liệu không thoả mãn (gọi là Data Filtering hay Partition Pruning). Ngược lại, CSV yêu cầu quét tuần tự toàn bộ file cho đến khi tìm đủ 10 dòng thoả mãn điều kiện.

*(Tip: Để thoát khỏi giao diện Trino, gõ `quit` hoặc nhấn `Ctrl + D`)*

---

### 🧹 Bước 5: Dọn dẹp hệ thống

Khi hoàn tất bài Lab, để tắt hệ thống và giải phóng tài nguyên (xóa các container và dữ liệu tạm), chạy lệnh:
```bash
docker-compose down -v
```
*(Cờ `-v` giúp xoá luôn các volume chứa data tạm trên Docker để dọn dẹp dung lượng)*
