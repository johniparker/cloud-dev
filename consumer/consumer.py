import boto3
import json
import time
import logging
import argparse
from moto import mock_aws
# Configure logging
logging.basicConfig(filename='consumer.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

class Consumer:
    def __init__(self, queue_name=None, request_bucket=None, storage_bucket=None, table_name=None):
        """
        Initialize the Consumer with bucket names and table name.
        :param queue_name: Queue containing incoming messages/requests.
        :param request_bucket: Bucket containing incoming requests.
        :param storage_bucket: Bucket to store processed widgets (Bucket 3).
        :param table_name: DynamoDB table name.
        """
        self.s3 = boto3.client('s3')
        self.dynamodb = boto3.resource('dynamodb')
        self.sqs = boto3.client('sqs')
        
        self.queue_name = queue_name
        self.request_bucket = request_bucket
        self.storage_bucket = storage_bucket
        self.table_name = table_name
        self.table = self.dynamodb.Table(self.table_name)
        self.message_cache = []
        self.queue_url = None
        
        if self.queue_name:
            try:
                # Attempt to get the queue URL
                response = self.sqs.get_queue_url(QueueName=self.queue_name)
                self.queue_url = response['QueueUrl']
                logging.info(f"Queue URL retrieved: {self.queue_url}")
            except self.sqs.exceptions.QueueDoesNotExist:
                logging.error(f"The queue '{self.queue_name}' does not exist.")
            except Exception as e:
                logging.error(f"Failed to retrieve queue URL: {e}")
        logging.info("Consumer initialized.")

    def poll_requests(self):
        empty_poll_count = 0
        max_empty_polls = 10
        
        while empty_poll_count < max_empty_polls:
            request_key = self.get_next_request()
            if request_key:
                self.process_request(request_key)
                self.s3.delete_object(Bucket=self.request_bucket, Key=request_key)
                logging.info(f"Processed and deleted request: {request_key}")
                empty_poll_count = 0
            else:
                empty_poll_count += 1
                time.sleep(0.1)
                
        logging.info("No more requests found. Exiting.")

    def get_next_request(self):
        response = self.s3.list_objects_v2(Bucket=self.request_bucket)
        if 'Contents' in response:
            sorted_objects = sorted(response['Contents'], key=lambda x: x['Key'])
            return sorted_objects[0]['Key'] if sorted_objects else None
        return None

    def process_request(self, key):
        obj = self.s3.get_object(Bucket=self.request_bucket, Key=key)
        request = json.loads(obj['Body'].read().decode('utf-8'))
        logging.info(f"Processing request: {request}")

        request_type = request.get("type")
        if not request_type:
            print('request is missing the type field.')
            return
        if request_type == 'create':
            self.handle_create_request(request)
        elif request_type == 'update':
            self.handle_update_request(request)
        elif request_type == 'delete':
            self.handle_delete_request(request)
        else:
            logging.warning(f"Unknown request type '{request_type}'. Ignoring.")

    def handle_create_request(self, request):
        widget = {
            'id': request['widget'].get('widgetId'),  # Map widgetId to id for DynamoDB
            'widgetId': request['widget'].get('widgetId'),
            'owner': request['widget'].get('owner'),
            'label': request['widget'].get('label'),
            'description': request['widget'].get('description'),
            'otherAttributes': request['widget'].get('otherAttributes')
        }
        # Ensure 'owner' is a string before calling replace
        owner = widget['owner']
        if isinstance(owner, str):
            owner = owner.replace(" ", "-").lower()
        else:
            logging.error(f"Invalid owner value: {owner}")
            return
            
        self.store_in_s3(widget)
        self.store_in_dynamodb(widget)
            
    def handle_update_request(self, request):
        widget_id = request['widget'].get('widgetId')
        updates = request['widget']
        if not widget_id:
            logging.error("Update request missing widgetId")
            return

        # Retrieve current widget from DynamoDB
        response = self.table.get_item(Key={'widgetId': widget_id})
        if 'Item' not in response:
            logging.error(f"Widget with id {widget_id} not found for update")
            return

        # Update attributes
        updated_widget = response['Item']
        updated_widget.update(updates)

        # Save updated widget back to DynamoDB
        self.table.put_item(Item=updated_widget)
        
        # Save updated widget back to S3
        self.s3.put_object(Bucket=self.storage_bucket, Key="widgets/test-user/1", Body=json.dumps(updated_widget))
        
    def handle_delete_request(self, request):
        widget_id = request['widget'].get('widgetId')
        if not widget_id:
            logging.error("Delete request missing widgetId")
            return

        # Delete widget from DynamoDB
        self.table.delete_item(Key={'widgetId': widget_id})

        # Optionally, delete related S3 object
        owner = request['widget'].get('owner', '').replace(" ", "-").lower()
        s3_key = f"widgets/{owner}/{widget_id}"
        self.s3.delete_object(Bucket=self.request_bucket, Key=s3_key)
        
    def store_in_s3(self, widget):
        owner = widget['owner'].replace(" ", "-").lower()
        key = f"widgets/{owner}/{widget['widgetId']}"
        self.s3.put_object(Bucket=self.storage_bucket, Key=key, Body=json.dumps(widget))
        logging.info(f"Stored widget in S3 at key: {key}")

    def store_in_dynamodb(self, widget):
        """
        Store a widget in the DynamoDB table with flattened attributes.
        :param widget: Widget data.
        """
        print('WIDGET: ', widget)
        flattened_widget = {
            'id': widget.get('id'),
            'widgetId': widget.get('widgetId'),
            'owner': widget.get('owner'),
            'label': widget.get('label'),
            'description': widget.get('description'),
            'otherAttributes': widget.get('otherAttributes')
        }
        
        other_attributes = widget.get('otherAttributes', {})
        if other_attributes:
            for key, value in other_attributes.items():
                flattened_widget[key] = value
          
        self.table.put_item(Item=flattened_widget)
        logging.info("Stored widget in DynamoDB")
    
    #get messages from SQS
    def get_messages_from_queue(self, max_messages=10):
        if not self.queue_url:
            logging.error("Queue URL is not set. Cannot retrieve messages.")
            return []
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=10  # Long polling to reduce empty responses
        )
        logging.info("recieved messages: ", response)
        return response.get('Messages', [])

    #delete message from SQS
    def delete_message_from_queue(self, receipt_handle):
        self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle
        )
        
    #retrieve the next queue message from the cache if we have one, otherwise retrieve from AWS SQS
    def get_next_message(self):
        if not self.message_cache:
            self.message_cache = self.get_messages_from_queue(max_messages=10)

        if self.message_cache:
            return self.message_cache.pop(0)  # Return and remove the next message
        return None

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run the S3-DynamoDB consumer.")
    parser.add_argument('--request-bucket', required=False, help="request bucket name")
    parser.add_argument('--storage-bucket', required=False, help="storage bucket name")
    parser.add_argument('--table-name', required=False, help="DynamoDB table name")
    parser.add_argument('--queue-name', required=False, help="SQS queue name")
    parser.add_argument('--strategy', choices=['polling', 'event-driven'], default='polling',
                        help="Storage strategy to use (default: polling)")

    args = parser.parse_args()
    # Instantiate and start the consumer
    consumer = Consumer(queue_name=args.queue_name, request_bucket=args.request_bucket, storage_bucket=args.storage_bucket, table_name=args.table_name)
    if args.strategy == 'polling':
        consumer.poll_requests()
    else:
        logging.info("Event-driven strategy is not implemented yet.")