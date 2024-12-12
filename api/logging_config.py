import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,  # Set to DEBUG to capture all log levels
        format='%(asctime)s:%(levelname)s:%(message)s',  # Log format
        handlers=[
            logging.StreamHandler(),  # Output to console
            logging.FileHandler("request_handler.log")  # Optionally log to a file
        ]
    )

