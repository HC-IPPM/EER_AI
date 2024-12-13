# EER_AI
The EER AI project is to help assess submissions from open and targeted organization and individuals or provide feedback to the EER Panel. The panel is composed of 13 experts from various fields across the health sector, who review domestic and international approaches and best practices for the development of preventive health care guidelines, and engage domestic and international experts and stakeholders. The goal is to elicit responses for recommendations related to the Task Force

/input
 - prompt file
 - input csv

/bucket.py
 - load input csv into google cloud bucket
 - each submission is a separate blob

/query.py (not finished)
 - covert submission text into embedding and store in vectorsm database
 - query client question against the vector database

/summary.py (not finished)
 - generate summary of submissions, output into csv file