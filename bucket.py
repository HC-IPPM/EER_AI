from google.cloud import storage

def upload_csv_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a CSV file to a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        source_file_name (str): Local path to the CSV file.
        destination_blob_name (str): Desired name for the file in the bucket.
    """
    # Initialize the GCS client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob (object) in the bucket
    blob = bucket.blob(destination_blob_name)

    # Upload the file
    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {bucket_name}/{destination_blob_name}.")

# Example usage
if __name__ == "__main__":
    # Replace these values with your own
    BUCKET_NAME = "your-bucket-name"
    SOURCE_FILE_NAME = "path/to/your-file.csv"
    DESTINATION_BLOB_NAME = "folder-in-bucket/your-file.csv"  # Optional folder structure

    upload_csv_to_gcs(BUCKET_NAME, SOURCE_FILE_NAME, DESTINATION_BLOB_NAME)
