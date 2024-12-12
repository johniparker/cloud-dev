import boto3
import logging
import json
from api.logging_config import setup_logging

setup_logging()
# Initialize SQS client
sqs = boto3.client('sqs')

def send_to_queue(request_body):
    queue_name = request_body.get('queueName')
    try:
        # Attempt to get the queue URL
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        logging.info(f"Queue URL retrieved: {queue_url}")
    except sqs.exceptions.QueueDoesNotExist:
        logging.error(f"The queue '{queue_name}' does not exist.")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"The queue '{queue_name}' does not exist."})
        }
    except Exception as e:
        logging.error(f"Failed to retrieve queue URL: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to retrieve queue URL."})
        }

    # Prepare the message body by excluding the 'queueName' key
    message_body = {key: value for key, value in request_body.items() if key != 'queueName'}

    try:
        # Send the message to the queue
        send_response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body)
        )
        message_id = send_response.get('MessageId')
        logging.info(f"Message sent successfully. Message ID: {message_id}")
        return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Message sent successfully.",
                    "message_id": message_id,
                    "queue_name": queue_name
                })
            }
    except Exception as e:
        logging.error(f"Failed to send message: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to send message to queue."})
        }