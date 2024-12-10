import json
import uuid
import logging
from api.helpers.validator import validate_widget_request
from api.helpers.sqs_client import send_to_queue
from api.logging_config import setup_logging

setup_logging()

def request_handler(event):
    logging.info(f"Received event: {event}")
    try:
        # Parse the incoming request
        body = json.loads(event.get('body'))
        queue_name = body["queueName"]
        
        # Add a unique request ID if not already present
        if "requestId" not in body:
            body["requestId"] = str(uuid.uuid4())
            
        # Validate the request
        validate_widget_request(body)

        # Process the request (send to SQS)
        send_response = send_to_queue(body)
        message_id = send_response['body']['message_id']
        # Return success response
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Widget Request submitted successfully.",
                "message_id": message_id,
                "queue_name": queue_name
            })
        }

    except ValueError as e:
        logging.error(f"Failed to parse request body: {e}")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }

    except Exception as e:
        logging.error(f"internal error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal Server Error"})
        }
