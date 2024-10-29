import boto3

# Create a session with AWS
session = boto3.Session()

# Create an S3 client using the session
s3 = session.client('s3')

# List all S3 buckets
response = s3.list_buckets()

# Print the names of all the buckets
print("S3 Buckets:")
for bucket in response['Buckets']:
    print(f"  - {bucket['Name']}")
