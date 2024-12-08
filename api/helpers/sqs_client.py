import boto3
import uuid

# Initialize SQS client
sqs = boto3.client('sqs')
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/WidgetQueue"

def send_to_queue(request_body, queue_name):
    request_id = str(uuid.uuid4())
    message = {
        "request_id": request_id,
        **request_body
    }
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message)
    )
    return request_id
