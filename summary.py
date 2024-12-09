from google.cloud import storage
from google.cloud import aiplatform
import os
import vertexai
from vertexai.language_models import TextGenerationModel
import pandas as pd

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

def summarize_text_with_vertexai(text, project_id, location):
    """
    Summarizes a given text using Vertex AI's generative models.

    Args:
        text (str): The text to summarize.
        project_id (str): Google Cloud project ID.
        location (str): Location of the Vertex AI model (default: "us-central1").

    Returns:
        str: Summary of the input text.
    """
    # Initialize Vertex AI
    vertexai.init(project=project_id, location=location)
    parameters = {
        "top_p": 0.95,
        "top_k": 40,
    }

    # Load the text generation model
    model = TextGenerationModel.from_pretrained("publishers/google/models/gemini-pro")
    # Summarize the text
    response = model.predict(
        prompt=f"Summarize the following text:\n{text}",
        temperature=0.2,
        max_output_tokens=256,
    )
    return response.text


def main(bucket_name, prefix, project_id, location):
    # List text files in the bucket
    text_files = list_text_files_in_gcs(bucket_name, prefix)
    print(f"Found {len(text_files)} text files.")

    # List text files in the bucket
    text_files = list_text_files_in_gcs(bucket_name, prefix)
    print(f"Found {len(text_files)} text files.")

    summary_df = pd.DataFrame()

    # Process each file
    for file_name in text_files:
        print(f"Processing file: {file_name}")

        # Read the content of the file
        text_content = read_text_file_from_gcs(bucket_name, file_name)

        # Summarize the content using Vertex AI
        summary = summarize_text_with_vertexai(text_content, project_id, location)
        new_row = {file_name:summary}
        summary_df = summary_df.append(new_row, ignore_index=True)
        # Print or save the summary
        print(f"Summary for {file_name}")
    
    # export summary
    summary_df.to_csv('output/summary.csv', index=False)

if __name__ == "__main__":
    # Configuration
    BUCKET_NAME = "eer_ai_input_20247253"
    PREFIX = "submissions/"  # Optional folder prefix in the bucket
    PROJECT_ID = "phx-01jd7g1rw9j"
    LOCATION = "northamerica-northeast1"
    main(BUCKET_NAME, PREFIX, PROJECT_ID, LOCATION)
