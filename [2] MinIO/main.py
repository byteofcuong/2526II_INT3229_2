import boto3
from botocore.client import Config

s3 = boto3.resource('s3',
                    endpoint_url='http://localhost:9000',
                    aws_access_key_id='admin',
                    aws_secret_access_key='password123',
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

bucket_name = 'my-test-bucket'
file_path = 'test.txt'
object_name = 'test-file-tren-minio.txt'
download_path = 'downloaded_test.txt'

print("--- BẮT ĐẦU TEST S3 REST API ---")

print(f"\n1. Đang tạo bucket: '{bucket_name}'...")
try:
    s3.create_bucket(Bucket=bucket_name)
    print(" -> Tạo thành công!")
except Exception as e:
    print(f" -> Bucket có thể đã tồn tại: {e}")

print(f"\n2. Đang upload file '{file_path}' lên MinIO...")
s3.meta.client.upload_file(file_path, bucket_name, object_name)
print(" -> Upload thành công!")

print("\n3. Lấy danh sách các file đang có trong bucket:")
bucket = s3.Bucket(bucket_name)
for obj in bucket.objects.all():
    print(f" -> Tìm thấy file: {obj.key}")

print(f"\n4. Đang tải file về máy với tên '{download_path}'...")
s3.meta.client.download_file(bucket_name, object_name, download_path)
print(" -> Tải về thành công!")

print(f"\n5. Đang xóa file '{object_name}'...")
s3.Object(bucket_name, object_name).delete()
print(" -> Xóa thành công!")

print("\n--- HOÀN THÀNH ---")