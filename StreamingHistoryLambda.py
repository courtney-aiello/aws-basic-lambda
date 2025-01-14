# Adds up msPlayed from .csv file and maps to artistName to create a total

import boto3
import csv
import io
from collections import defaultdict

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # Extract bucket name and key from the event
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print(f"Processing file from bucket: {bucket_name}, key: {key}")
    except KeyError as e:
        print(f"Error extracting bucket/key: {e}")
        return {"statusCode": 400, "body": "Invalid S3 event structure"}
    
    # Download the CSV file from S3
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        print(f"Successfully read CSV file: {key}")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return {"statusCode": 500, "body": "Failed to read CSV file"}
    
    # Process the CSV file and calculate totals
    artist_totals = defaultdict(int)
    try:
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        for row in csv_reader:
            artist_name = row['artistName']
            ms_played = int(row['msPlayed'])
            artist_totals[artist_name] += ms_played
        print("Successfully calculated totals")
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        return {"statusCode": 500, "body": "Failed to process CSV file"}
    
    # Write the results to a new CSV file
    try:
        # Prepare CSV in-memory
        output_csv_buffer = io.StringIO()
        writer = csv.writer(output_csv_buffer)
        
        # Write header and data
        writer.writerow(['artistName', 'totalMsPlayed'])
        for artist, total in artist_totals.items():
            writer.writerow([artist, total])
        
        # Define the output key for the processed CSV
        output_key = key.replace('raw-data/', 'processed-data/').replace('.csv', '_totals.csv')
        
        # Upload the CSV back to S3
        s3.put_object(Bucket=bucket_name, Key=output_key, Body=output_csv_buffer.getvalue())
        print(f"CSV file uploaded to: {output_key}")
    except Exception as e:
        print(f"Error uploading CSV file: {e}")
        return {"statusCode": 500, "body": "Failed to upload CSV file"}
    
    return {"statusCode": 200, "body": f"File successfully processed and uploaded to {output_key}"}
