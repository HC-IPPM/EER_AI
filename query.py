from google.cloud import storage
import vertexai
from vertexai.language_models import TextEmbeddingModel
import pinecone

def embed_and_store_submissions(bucket_name, vector_db_name, project_id, location):
    """
    Embeds submissions from a GCS bucket and stores them in a vector database.

    Args:
        bucket_name (str): Name of the GCS bucket containing submissions.
        vector_db_name (str): Name of the vector database.
        project_id (str): Google Cloud project ID.
    """
    # Initialize Vertex AI and GCS
    vertexai.init(project=project_id, location=location)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Load embeddings model
    embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko")

    # Initialize Pinecone
    pinecone.init(api_key="your-pinecone-api-key", environment="us-west1-gcp")
    if vector_db_name not in pinecone.list_indexes():
        pinecone.create_index(vector_db_name, dimension=768)
    index = pinecone.Index(vector_db_name)

    # Process files in the GCS bucket
    blobs = bucket.list_blobs()
    for blob in blobs:
        if blob.name.endswith(".txt"):
            text = blob.download_as_text()
            embedding = embedding_model.get_embeddings([text]).embeddings[0]

            # Store in vector database
            index.upsert([(blob.name, embedding, {"text": text})])
            print(f"Embedded and stored: {blob.name}")

def query_and_answer(question, vector_db_name, project_id, location):
    """
    Queries the vector database and generates an answer using retrieved context.

    Args:
        question (str): The question to query against the submissions.
        vector_db_name (str): Name of the vector database.
        project_id (str): Google Cloud project ID.

    Returns:
        str: The generated answer.
    """
    # Initialize Vertex AI
    vertexai.init(project=project_id, location=location)

    # Load embeddings model
    embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko")
    query_embedding = embedding_model.get_embeddings([question]).embeddings[0]

    # Query the vector database
    pinecone.init(api_key="your-pinecone-api-key", environment="us-west1-gcp")
    index = pinecone.Index(vector_db_name)
    results = index.query(query_embedding, top_k=3, include_metadata=True)

    # Combine the most relevant submissions as context
    context = " ".join([item["metadata"]["text"] for item in results["matches"]])

    # Generate answer using the text generation model
    generation_model = TextGenerationModel.from_pretrained("text-bison")
    answer = generation_model.predict(
        prompt=f"Context:\n{context}\n\nQuestion: {question}\nAnswer:",
        temperature=0.2,
        max_output_tokens=256,
    )
    return answer.text

if __name__ == "__main__":
    question = "What are the key concerns raised in the submissions?"
    answer = query_and_answer(question, "submissions-vectors", "your-project-id")
    print(f"Answer: {answer}")

