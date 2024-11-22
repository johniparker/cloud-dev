FROM python:3.9-slim
WORKDIR /cloud-dev
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY /consumer/consumer.py consumer.py
CMD ["python", "consumer.py", "--queue-name", "cs5250-requests", "--request-bucket", "usu-cs5250-green-requests", "--storage-bucket", "usu-cs5250-green-web", "--table-name", "widgets", "--strategy", "polling"]