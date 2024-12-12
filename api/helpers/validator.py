from jsonschema import validate, ValidationError
import json
import logging
from api.logging_config import setup_logging

setup_logging()
# Define JSON schema for the Widget Request
WIDGET_REQUEST_SCHEMA = {
    "type": "object",
    "properties": {
        "queueName": {"type": "string"},
        "requestId": {"type": "string"},
        "widgetId": {"type": "string"},
        "owner": {"type": "string"},
        "label": {"type": "string"},
        "description": {"type": "string"},
        "otherAttributes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "value": {"type": "string"}
                },
                "required": ["name", "value"]
            }
        }
    },
    "required": ["queueName", "requestId", "widgetId"]
}

def validate_widget_request(request_body):
    try:
        validate(instance=request_body, schema=WIDGET_REQUEST_SCHEMA)
    except ValidationError as e:
        logging.error(f"Validation failed: {e.message}")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Invalid request: {e.message}"})
        }
    logging.info("Request validation passed.")
    return None
