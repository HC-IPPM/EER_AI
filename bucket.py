import pandas as pd
from google.cloud import storage
import random

def generate_bucket_number(bucket_name):
    """
    generate random bucket number for the bucket name so that the name is globally unique

    Args:
        bucket_name (str): Name of the GCS bucket.
    """   
    temp = random.randint(1000, 9999)
    new_bucket_name = bucket_name + str(temp)
    return new_bucket_name

def create_bucket(bucket_name, project_id):
    """
    create a google cloud bucket

    Args:
        bucket_name (str): Name of the GCS bucket.
    """   
    # Initialize a storage client
    client = storage.Client(project=project_id)

    # Create a new bucket
    bucket = client.create_bucket(bucket_name)

    print(f"Bucket {bucket.name} created.")

def upload_rows_to_gcs(bucket_name, source_csv_path, project_id, blob_name_column=None, folder_prefix=""):
    """
    Uploads each row of a CSV file as a new blob in a GCS bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        source_csv_path (str): Path to the local CSV file.
        blob_name_column (str): Optional. Name of the column to use for blob names. 
                                If None, unique identifiers will be generated.
        folder_prefix (str): Optional. Folder prefix for uploaded blobs in GCS.
    """
     # Initialize the GCS client with the specified project ID
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    # Load the CSV into a pandas DataFrame
    df = pd.read_csv(source_csv_path, encoding="latin")

    # Process each row in the DataFrame
    for index, row in df.iterrows():
        # Generate blob name
        if blob_name_column and blob_name_column in df.columns:
            blob_name = f"{folder_prefix}{row[blob_name_column]}.txt"
        else:
            blob_name = f"{folder_prefix}row_{index + 1}.txt"

        # Prepare row content as text
        row_content = "\n".join([f"{key}: {value}" for key, value in row.items()])

        # Create a blob (object) in the bucket
        blob = bucket.blob(blob_name)

        # Upload the row as a blob
        blob.upload_from_string(row_content, content_type="text/plain")

        print(f"Uploaded row {index + 1} as {blob_name}")

if __name__ == "__main__":
    # Configuration
    BUCKET_NAME = "eer_ai_input_2024"
    SOURCE_CSV_PATH = "input/ResponseLog(EER Submission).csv"
    BLOB_NAME_COLUMN = "unique_identifier"  # Replace with a column name from your CSV, or set to None
    FOLDER_PREFIX = "submissions/"  # Optional folder inside the bucket
    PROJECT_ID = "phx-01jd7g1rw9j"

    # Upload rows to GCS
    NEW_BUCKET_NAME= generate_bucket_number(BUCKET_NAME)
    create_bucket(NEW_BUCKET_NAME, PROJECT_ID)
    upload_rows_to_gcs(NEW_BUCKET_NAME, SOURCE_CSV_PATH, PROJECT_ID, BLOB_NAME_COLUMN, FOLDER_PREFIX)
