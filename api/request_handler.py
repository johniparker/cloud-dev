import json
import uuid
from helpers.validator import validate_widget_request
from helpers.sqs_client import send_to_queue

def request_handler(event):
    try:
        # Parse the incoming request
        request_body = json.loads(event.get('body', '{}'))

        # Check if the queue name is provided
        queue_name = request_body.get("queueName")
        if not queue_name:
            raise ValueError("Missing 'queueName' in the request body.")
        
        # Add a unique request ID if not already present
        if "requestId" not in request_body:
            request_body["requestId"] = str(uuid.uuid4())
            
        # Validate the request
        validate_widget_request(request_body)

        # Process the request (send to SQS)
        message_id = send_to_queue(request_body)

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
        # Handle validation errors
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }

    except Exception as e:
        # Handle unexpected errors
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal Server Error"})
        }
