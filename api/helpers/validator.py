from jsonschema import validate, ValidationError

# Define JSON schema for the Widget Request
WIDGET_REQUEST_SCHEMA = {
    "type": "object",
    "properties": {
        "widget_id": {"type": "string"},
        "widget_type": {"type": "string"},
        "quantity": {"type": "integer", "minimum": 1},
        "priority": {"type": "string", "enum": ["low", "medium", "high"]}
    },
    "required": ["widget_id", "widget_type", "quantity", "priority"]
}

def validate_widget_request(request_body):
    try:
        validate(instance=request_body, schema=WIDGET_REQUEST_SCHEMA)
    except ValidationError as e:
        raise ValueError(f"Invalid request: {e.message}")
