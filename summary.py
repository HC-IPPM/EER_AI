from google.cloud import storage
from google.cloud import aiplatform
import os

def list_text_files_in_gcs(bucket_name, prefix=""):
    """
    Lists text files in a specified GCS bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        prefix (str): Optional prefix to filter files (e.g., folder name).

    Returns:
        list: List of text file names in the bucket.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    # Filter for .txt files
    return [blob.name for blob in blobs if blob.name.endswith(".txt")]

def read_text_file_from_gcs(bucket_name, file_name):
    """
    Reads the content of a text file from GCS.

    Args:
        bucket_name (str): Name of the GCS bucket.
        file_name (str): Path to the file in the bucket.

    Returns:
        str: Content of the text file.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download the content as a string
    return blob.download_as_text()

def summarize_text(text, model_endpoint, project_id, location="us-central1"):
    """
    Summarizes a given text using Vertex AI's generative models.

    Args:
        text (str): The text to summarize.
        model_endpoint (str): Vertex AI model endpoint.
        project_id (str): Google Cloud project ID.
        location (str): Location of the Vertex AI model (default: "us-central1").

    Returns:
        str: Summary of the input text.
    """
    aiplatform.init(project=project_id, location=location)

    # Initialize the prediction service
    endpoint = aiplatform.Endpoint(model_endpoint)

    # Send the text for summarization
    response = endpoint.predict(instances=[{"content": text}])
    summary = response.predictions[0].get("summary", "Summary not available")

    return summary

def main():
    # Configuration
    BUCKET_NAME = "your-bucket-name"
    PREFIX = "text_files/"  # Optional folder prefix in the bucket
    PROJECT_ID = "your-project-id"
    MODEL_ENDPOINT = "projects/your-project-id/locations/us-central1/endpoints/your-endpoint-id"

    # List text files in the bucket
    text_files = list_text_files_in_gcs(BUCKET_NAME, PREFIX)
    print(f"Found {len(text_files)} text files.")

    # Process each file
    for file_name in text_files:
        print(f"Processing file: {file_name}")
        
        # Read the content of the file
        text_content = read_text_file_from_gcs(BUCKET_NAME, file_name)

        # Summarize the content
        summary = summarize_text(text_content, MODEL_ENDPOINT, PROJECT_ID)

        # Print or save the summary
        print(f"Summary for {file_name}:\n{summary}\n")

if __name__ == "__main__":
    main()
