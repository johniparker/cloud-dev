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
    def __init__(self, bucket_name, table_name):
        self.bucket_name = bucket_name
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
                self.s3.delete_object(Bucket=self.bucket_name, Key=request_key)
                logging.info(f"Processed and deleted request: {request_key}")
                empty_poll_count = 0
            else:
                empty_poll_count += 1
                time.sleep(0.1)
                
        logging.info("No more requests found. Exiting.")

    def get_next_request(self):
        response = self.s3.list_objects_v2(Bucket=self.bucket_name)
        if 'Contents' in response:
            sorted_objects = sorted(response['Contents'], key=lambda x: x['Key'])
            return sorted_objects[0]['Key'] if sorted_objects else None
        return None

    def process_request(self, key):
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=key)
        request = json.loads(obj['Body'].read().decode('utf-8'))
        logging.info(f"Processing request: {request}")

        request_type = request.get("type")
        if not request_type:
            print('request is missing the type field.')
            return
        if request_type == 'create':
            widget = {
                'id': request.get('widgetId'),
                'widgetId': request.get('widgetId'),  # Map widgetId to id for DynamoDB
                'owner': request.get('owner'),
                'label': request.get('label'),
                'description': request.get('description'),
                'attributes': request.get('otherAttributes')
            }
            if 'owner' not in widget:
                print('widget is missing an owner')
                return
            
            self.store_in_s3(widget)
            self.store_in_dynamodb(widget)
        else:
            print(f"warning: unknown request type '{request_type}")

    def store_in_s3(self, widget):
        owner = widget['owner'].replace(" ", "-").lower()
        key = f"widgets/{owner}/{widget['widgetId']}"
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=json.dumps(widget))
        logging.info(f"Stored widget in S3 at key: {key}")

    def store_in_dynamodb(self, widget):
        table = self.dynamodb.Table(self.table_name)
        table.put_item(Item=widget)
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