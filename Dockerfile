FROM python:3.9-slim
COPY /consumer/consumer.py consumer.py
CMD ["python", "consumer.py"]