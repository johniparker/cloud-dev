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
        request_body = json.loads(event.get('body'))
        
        # Add a unique request ID if not already present
        if "requestId" not in request_body:
            request_body["requestId"] = str(uuid.uuid4())
            
        # Validate the request
        validation_response = validate_widget_request(request_body)
        if validation_response:
            return validation_response

        # Process the request (send to SQS)
        send_response = send_to_queue(request_body)
        if send_response['statusCode'] != 200:
            return send_response  # Return error from send_to_queue directly

        # Extract response details for success
        response_body = json.loads(send_response['body'])
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Widget Request submitted successfully.",
                "message_id": response_body["message_id"],
                "queue_name": response_body["queue_name"]
            })
        }
    except Exception as e:
        logging.error(f"internal error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal Server Error"})
        }
