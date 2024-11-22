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
        self.sqs = boto3.client('sqs')
        self.dynamodb = boto3.resource('dynamodb')

        # Define test buckets and table
        self.request_bucket = 'test-request-bucket'
        self.storage_bucket = 'test-storage-bucket'
        self.table_name = 'widgets'
        self.queue_name = 'message-queue'
        queue = self.sqs.create_queue(QueueName=self.queue_name)
        self.queue_url = queue['QueueUrl']
        # Create bucket and table
        self.s3.create_bucket(Bucket=self.request_bucket)
        self.s3.create_bucket(Bucket=self.storage_bucket)
        try:
            self.table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
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
            "--queue-name", self.queue_name,
            "--request-bucket", self.request_bucket,
            "--storage-bucket", self.storage_bucket,
            "--table-name", self.table_name
            
        ]
        # Initialize the Consumer with the test buckets and table
        self.consumer = Consumer(
            queue_name=self.queue_name,
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
        widget = {'widgetId': '1', 'owner': 'Test User', 'otherAttributes': [{'name': 'other', 'value': 'other'}]}
        expected_widget = {
            'id': '1',
            'widgetId': '1',
            'owner': 'Test User',
            'label': None,
            'description': None,
            'other': 'other'
        }
        self.s3.put_object(Bucket=self.request_bucket, Key='request1', Body=json.dumps({'type': 'create', **widget}))

        # Process the request
        self.consumer.process_request('request1')
        
        # Check if the widget is stored in S3
        try:
            result = self.s3.get_object(Bucket=self.storage_bucket, Key="widgets/test-user/1")
            stored_widget = json.loads(result['Body'].read().decode('utf-8'))
            self.assertEqual(stored_widget, expected_widget)
        except self.s3.exceptions.ClientError as e:
            self.fail(f"Failed to retrieve object from S3: {e}")

    
    def test_store_in_dynamodb(self):
        widget = {'id': '1', 'widgetId': '1', 'owner': 'Test User', 'otherAttributes': [{'name': 'other', 'value': 'other'}]}
        expected_widget = {
            'id': '1',
            'widgetId': '1',
            'owner': 'Test User',
            'label': None,
            'description': None,
            'other': 'other'
        }
        self.consumer.store_in_dynamodb(widget)

        # Retrieve from DynamoDB and verify
        response = self.table.get_item(Key={'id': '1'})
        self.assertEqual(response['Item'], expected_widget)

   
    def test_store_in_s3(self):
        widget = {'id': '1', 'widgetId': '1', 'owner': 'Test User', 'otherAttributes': [{'name': 'other', 'value': 'other'}]}
        expected_widget = {
            'id': '1',
            'widgetId': '1', 
            'owner': 'Test User', 
            'label': None,
            'description': None,
            'other': 'other'
        }
        self.consumer.store_in_s3(widget)

        # Check if stored in S3 with correct key
        result = self.s3.get_object(Bucket=self.storage_bucket, Key='widgets/test-user/1')
        stored_widget = json.loads(result['Body'].read().decode('utf-8'))
        self.assertEqual(stored_widget, expected_widget)
        
    def test_get_next_message(self):
        # Add a message to the queue
        
        self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps({'type': 'create', 'widgetId': '1', 'owner': 'Test User', 'otherAttributes': [{'name': 'other', 'value': 'other'}]})
        )

        message = self.consumer.get_next_message()
        self.assertIsNotNone(message)
        self.assertEqual(json.loads(message['Body'])['type'], 'create')

        # Delete the message after processing
        self.consumer.delete_message_from_queue(message['ReceiptHandle'])
        
    def test_handle_update_request(self):
        # Pre-insert a widget into DynamoDB
        self.table.put_item(Item={
            'id': '1',
            'widgetId': '1',
            'owner': 'Test User',
            'label': 'Old Label',
            'description': 'Old Description',
            'otherAttributes': [{'name': 'other', 'value': 'other'}]
        })

        # Update request payload
        update_request = {
            'type': 'update',
            'widgetId': '1',
            'description': 'Updated Description',
            'newAttribute': 'New Value',
            'otherAttributes': [{'name': 'other', 'value': 'new'}]
        }

        # Call the update handler
        self.consumer.handle_update_request(update_request)

        # Verify the updated widget in DynamoDB
        response = self.table.get_item(Key={'id': '1'})
        self.assertIn('Item', response)
        updated_widget = response['Item']

        self.assertEqual(updated_widget['widgetId'], '1')
        self.assertEqual(updated_widget['label'], 'Old Label')
        self.assertEqual(updated_widget['description'], 'Updated Description')
        self.assertEqual(updated_widget['newAttribute'], 'New Value')
        self.assertEqual(updated_widget['other'], 'new')

    def test_handle_delete_request(self):
        # Pre-insert a widget into DynamoDB
        self.table.put_item(Item={
            'id': '1',
            'widgetId': '1',
            'owner': 'Test User',
            'label': 'Label to be deleted',
            'description': 'Description to be deleted',
            'otherAttributes': [{'name': 'other', 'value': 'other'}]
        })

        # Add an S3 object corresponding to the widget
        self.s3.put_object(Bucket=self.request_bucket, Key='widgets/test-user/1', Body=json.dumps({'dummy': 'data'}))

        # Delete request payload
        delete_request = {
            'type': 'delete',
            'widgetId': '1',
            'owner': 'Test User'
        }

        # Call the delete handler
        self.consumer.handle_delete_request(delete_request)

        # Verify the widget is removed from DynamoDB
        response = self.table.get_item(Key={'id': '1'})
        self.assertNotIn('Item', response)

        # Verify the S3 object is deleted
        with self.assertRaises(self.s3.exceptions.NoSuchKey):
            self.s3.get_object(Bucket=self.storage_bucket, Key='widgets/test-user/1')
            
if __name__ == '__main__':
    unittest.main()