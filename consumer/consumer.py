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
    def __init__(self, request_bucket, storage_bucket, table_name):
        """
        Initialize the Consumer with bucket names and table name.
        :param request_bucket: Bucket containing incoming requests.
        :param storage_bucket: Bucket to store processed widgets (Bucket 3).
        :param table_name: DynamoDB table name.
        """
        self.request_bucket = request_bucket
        self.storage_bucket = storage_bucket
        self.table_name = table_name
        self.s3 = boto3.client('s3')
        self.dynamodb = boto3.resource('dynamodb')
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
        else:
            logging.warning(f"Unknown request type '{request_type}'. Ignoring.")

    def handle_create_request(self, request):
        widget = {
                'id': request.get('widgetId'),
                'widgetId': request.get('widgetId'),  # Map widgetId to id for DynamoDB
                'owner': request.get('owner'),
                'label': request.get('label'),
                'description': request.get('description'),
                'attributes': request.get('otherAttributes')
            }
        if 'owner' not in widget or widget['owner'] is None:
            print('widget is missing an owner')
            return
        
        # Ensure 'owner' is a string before calling replace
        owner = widget['owner']
        if isinstance(owner, str):
            owner = owner.replace(" ", "-").lower()
        else:
            logging.error(f"Invalid owner value: {owner}")
            return
            
        self.store_in_s3(widget)
        self.store_in_dynamodb(widget)
            
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
        table = self.dynamodb.Table(self.table_name)
        flattened_widget = widget.copy()
        flattened_widget.update(widget.pop('attributes', {}))  # Flatten otherAttributes
        table.put_item(Item=flattened_widget)
        logging.info("Stored widget in DynamoDB")
    

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run the S3-DynamoDB consumer.")
    parser.add_argument('--bucket', required=True, help="S3 bucket name")
    parser.add_argument('--table', required=True, help="DynamoDB table name")
    parser.add_argument('--strategy', choices=['polling', 'event-driven'], default='polling',
                        help="Storage strategy to use (default: polling)")

    args = parser.parse_args()
    # Instantiate and start the consumer
    consumer = Consumer(bucket_name=args.bucket, table_name=args.table)
    if args.strategy == 'polling':
        consumer.poll_requests()
    else:
        logging.info("Event-driven strategy is not implemented yet.")