FROM python:3.9-slim
COPY /consumer/consumer.py consumer.py
CMD ["python", "consumer.py", "--queue-name", "cs5250-requests", "--request-bucket", "cs5250-green-requests", "--storage-bucket", "cs5250-green-web", "--table-name", "widgets"]