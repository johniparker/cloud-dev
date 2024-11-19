import sys
import unittest
from moto import mock_aws
import boto3
import json
import time
from consumer.consumer import Consumer

class TestConsumer(unittest.TestCase):
    def setUp(self):
        #start mocks
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        
        # Setup mock S3 and DynamoDB
        self.s3 = boto3.client('s3')
        self.dynamodb = boto3.resource('dynamodb')

        # Define test buckets and table
        self.request_bucket = 'test-request-bucket'
        self.storage_bucket = 'test-storage-bucket'
        self.table_name = 'widgets'

        # Create bucket and table
        self.s3.create_bucket(Bucket=self.request_bucket)
        self.s3.create_bucket(Bucket=self.storage_bucket)
        try:
            self.table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[{'AttributeName': 'widgetId', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'widgetId', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
            )
            # Wait for the table to be created
            self.table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
        except self.dynamodb.meta.client.exceptions.ResourceInUseException:
            # If the table already exists, get a reference to it
            self.table = self.dynamodb.Table(self.table_name)
            
         # Set command-line arguments for the Consumer
        sys.argv = [
            "consumer_script.py",  # Simulate script name
            "--request-bucket", self.request_bucket,
            "--storage-bucket", self.storage_bucket,
            "--table-name", self.table_name
        ]
        # Initialize the Consumer with the test buckets and table
        self.consumer = Consumer(
            request_bucket=self.request_bucket,
            storage_bucket=self.storage_bucket,
            table_name=self.table_name
        )

    
    def test_get_next_request(self):
        # Add a request object to S3
        self.s3.put_object(Bucket=self.request_bucket, Key='request1', Body=json.dumps({'type': 'create', 'widget': {'widgetId': '1', 'owner': 'Test User'}}))
        next_request = self.consumer.get_next_request()
        self.assertEqual(next_request, 'request1')

   
    def test_process_request(self):
        # Add a request object to S3
        self.s3.put_object(Bucket=self.request_bucket, Key='request1', Body=json.dumps({'type': 'create', 'widget': {'widgetId': '1', 'owner': 'Test User'}}))

        # Process the request
        self.consumer.process_request('request1')
        
        # Check the contents of the storage bucket to verify if the widget is there
        response = self.s3.list_objects_v2(Bucket=self.request_bucket)
        print("S3 Bucket contents:", response.get('Contents', []))  # This will print the contents of the storage bucket

        # Check if the widget is stored in S3
        try:
            result = self.s3.get_object(Bucket=self.storage_bucket, Key="widgets/test-user/1")
            stored_widget = json.loads(result['Body'].read().decode('utf-8'))
            self.assertEqual(stored_widget, widget)
        except self.s3.exceptions.ClientError as e:
            self.fail(f"Failed to retrieve object from S3: {e}")

    
    def test_store_in_dynamodb(self):
        widget = {'widgetId': '1', 'owner': 'Test User'}
        self.consumer.store_in_dynamodb(widget)

        # Retrieve from DynamoDB and verify
        response = self.table.get_item(Key={'widgetId': '1'})
        self.assertEqual(response['Item'], widget)

   
    def test_store_in_s3(self):
        widget = {'widgetId': '1', 'owner': 'Test User'}
        self.consumer.store_in_s3(widget)

        # Check if stored in S3 with correct key
        result = self.s3.get_object(Bucket=self.storage_bucket, Key='widgets/test-user/1')
        stored_widget = json.loads(result['Body'].read().decode('utf-8'))
        self.assertEqual(stored_widget, widget)

if __name__ == '__main__':
    unittest.main()