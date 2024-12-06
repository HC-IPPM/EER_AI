from google.cloud import language_v1

client = language_v1.LanguageServiceClient()

document = language_v1.Document(content="The policy changes are fantastic!", type_=language_v1.Document.Type.PLAIN_TEXT)

# Perform Sentiment Analysis
sentiment = client.analyze_sentiment(document=document).document_sentiment
print(f"Score: {sentiment.score}, Magnitude: {sentiment.magnitude}")
