
import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv()

MINIO_URL = os.getenv("MINIO_URL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

def get_minio_client(url: str, access_key: str, secret_key: str, secure: bool = False) -> Minio:
    return Minio(
        url,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )

def create_bucket_if_not_exists(client: Minio, bucket_name: str):
    try:
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except S3Error as exc:
        print(f"Error creating/checking MinIO bucket '{bucket_name}': {exc}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def main():
    """Main function to initialize MinIO client and create bucket."""
    minio_client = get_minio_client(MINIO_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    create_bucket_if_not_exists(minio_client, BUCKET_NAME)

if __name__ == "__main__":
    main() 