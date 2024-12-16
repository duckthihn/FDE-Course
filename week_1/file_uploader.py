from minio import Minio
from minio.error import S3Error
import os

def get_file_category(filename):
    """Determine the category based on file extension (case-insensitive)."""
    ext = filename.rsplit(".", 1)[-1].lower()
    if ext in ["csv", "parquet"]:
        return "structured"
    elif ext in ["json"]:
        return "semi-structured"
    else:
        return "unstructured"

def main():
    # Initialize MinIO client
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    bucket = "warehouse-script"

    # Check if bucket exists
    if client.bucket_exists(bucket):
        print(f"{bucket} already exists")
    else:
        print(f"{bucket} does not exist")
        print(f"Creating {bucket} bucket")
        client.make_bucket(bucket)

    upload_folder = "./data/upload/"
    download_folder = "./data/downloaded/"

    for filename in os.listdir(upload_folder):
        source_file = os.path.join(upload_folder, filename)
        category = get_file_category(filename)
        print(f"Processing file: {filename}, Category detected: {category}")  # Debugging

        destination_file = f"{category}/{filename}"  # Upload to categorized folder

        try:
            # Upload file
            client.fput_object(
                bucket,
                destination_file,
                source_file
            )
            print(
                source_file, "successfully uploaded as object",
                destination_file, "to bucket", bucket,
            )

            # Ensure local category folder exists for downloads
            local_download_path = os.path.join(download_folder, category)
            os.makedirs(local_download_path, exist_ok=True)

            # Download file
            client.fget_object(
                bucket,
                destination_file,
                os.path.join(local_download_path, filename)
            )
            print(
                destination_file, "successfully downloaded as object",
                filename, "from bucket", bucket,
            )
        except Exception as e:
            print(f"Error handling {filename}: {e}")

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("Error occurred.", exc)
