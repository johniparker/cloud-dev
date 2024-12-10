from jsonschema import validate, ValidationError
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
        # Validate the request body with the schema
        validate(instance=request_body, schema=WIDGET_REQUEST_SCHEMA)
        
        # Check for missing queueName or widgetId if schema is valid
        if "queueName" not in request_body:
            logging.error("Missing 'queueName' in the request body.")
            raise ValueError("Missing 'queueName' in the request body.")
        if "widgetId" not in request_body:
            logging.error("Missing 'widgetId' in the request body.")
            raise ValueError("Missing 'widgetId' in the request body.")
    except ValidationError as e:
        logging.error(f"Validation failed: {e.message}")
        raise ValueError(f"Invalid request: {e.message}")
    logging.info("Request validation passed.")
