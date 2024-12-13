import vertexai
from vertexai.language_models import TextEmbeddingModel, TextGenerationModel
from google.cloud import storage
import pinecone

# Initialize constants
PROJECT_ID = "phx-01jd7g1rw9j"  # Replace with your project ID
LOCATION = "northamerica-northeast1"  # Vertex AI location
BUCKET_NAME = "your-bucket-name"  # Replace with your GCS bucket name
VECTOR_DB_NAME = "submissions-vectors"  # Name of the Pinecone index
PINECONE_API_KEY = "your-pinecone-api-key"  # Replace with your Pinecone API key
PINECONE_ENVIRONMENT = "us-west1-gcp"  # Replace with your Pinecone environment

# Initialize Vertex AI
vertexai.init(project=PROJECT_ID, location=LOCATION)

# Initialize Pinecone
pinecone.init(api_key=PINECONE_API_KEY, environment=PINECONE_ENVIRONMENT)
if VECTOR_DB_NAME not in pinecone.list_indexes():
    pinecone.create_index(VECTOR_DB_NAME, dimension=768)
index = pinecone.Index(VECTOR_DB_NAME)

# Initialize GCS
storage_client = storage.Client()


def embed_submissions():
    """Embeds text submissions from a GCS bucket and stores them in a vector database."""
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()

    # Load embeddings model
    embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko")

    for blob in blobs:
        if blob.name.endswith(".txt"):
            # Download and embed text
            text = blob.download_as_text()
            embedding = embedding_model.get_embeddings([text]).embeddings[0]

            # Store in Pinecone
            index.upsert([(blob.name, embedding, {"text": text})])
            print(f"Embedded and stored: {blob.name}")


def query_submissions(question):
    """Queries the vector database and generates an answer using retrieved context."""
    # Load embeddings model
    embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko")
    query_embedding = embedding_model.get_embeddings([question]).embeddings[0]

    # Query the vector database
    results = index.query(query_embedding, top_k=3, include_metadata=True)

    # Combine the most relevant submissions as context
    context = "\n".join([item["metadata"]["text"] for item in results["matches"]])

    # Generate answer using the text generation model
    generation_model = TextGenerationModel.from_pretrained("text-bison")
    answer = generation_model.predict(
        prompt=f"Context:\n{context}\n\nQuestion: {question}\nAnswer:",
        temperature=0.2,
        max_output_tokens=256,
    )
    return answer.text


if __name__ == "__main__":
    # Step 1: Embed all submissions
    print("Embedding submissions from GCS...")
    embed_submissions()

    # Step 2: Query the embedded submissions
    question = "what are the key recommendations?"
    print(f"Querying submissions for: {question}")
    answer = query_submissions(question)
    print(f"Answer: {answer}")
