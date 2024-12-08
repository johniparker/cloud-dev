import json
from helpers.validator import validate_widget_request
from helpers.sqs_client import send_to_queue

def request_handler(event, context):
    try:
        # Parse the incoming request
        request_body = json.loads(event.get('body', '{}'))

        # Validate the request
        validate_widget_request(request_body)

        # Process the request (send to SQS)
        request_id = send_to_queue(request_body)

        # Return success response
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Widget Request submitted successfully.",
                "request_id": request_id
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
