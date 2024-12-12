import unittest
from unittest.mock import patch
from moto import mock_aws
import boto3
import json
from api.request_handler import request_handler

class TestRequestHandler(unittest.TestCase):
    def setUp(self):
        # Start Moto mock for AWS services
        self.mock_aws = mock_aws()
        self.mock_aws.start()

        # Initialize SQS client and create a mock queue
        self.sqs = boto3.client('sqs', region_name='us-east-1')
        self.queue_name = 'test-queue'
        queue = self.sqs.create_queue(QueueName=self.queue_name)
        self.queue_url = queue['QueueUrl']

    def tearDown(self):
        # Stop Moto mocks
        self.mock_aws.stop()

    def test_request_handler_success(self):
        # Valid request body
        request_body = {
            "queueName": self.queue_name,
            "widgetId": "widget123",
            "owner": "test-owner",
            "label": "Test Widget",
            "description": "A test widget",
            "otherAttributes": [{"name": "attr1", "value": "value1"}]
        }

        event = {"body": json.dumps(request_body)}

        response = request_handler(event)
        self.assertEqual(response["statusCode"], 200)

        body = json.loads(response["body"])
        self.assertIn("message", body)
        self.assertIn("message_id", body)
        self.assertEqual(body["queue_name"], self.queue_name)

    def test_request_handler_missing_queue_name(self):
        # Missing queueName in the request body
        request_body = {
            "widgetId": "widget123",
            "owner": "test-owner"
        }

        event = {"body": json.dumps(request_body)}

        response = request_handler(event)
        self.assertEqual(response["statusCode"], 400)

        body = json.loads(response["body"])
        self.assertIn("error", body)
        self.assertEqual(body["error"], "Invalid request: 'queueName' is a required property")

    def test_request_handler_invalid_request(self):
        # Invalid request with missing required fields
        request_body = {
            "queueName": self.queue_name,
            "owner": "test-owner"
        }

        event = {"body": json.dumps(request_body)}

        response = request_handler(event)
        self.assertEqual(response["statusCode"], 400)

        body = json.loads(response["body"])
        self.assertIn("error", body)
        self.assertTrue("Invalid request" in body["error"])

    def test_request_handler_queue_not_exist(self):
        # Queue does not exist
        request_body = {
            "queueName": "non-existent-queue",
            "widgetId": "widget123",
            "owner": "test-owner"
        }

        event = {"body": json.dumps(request_body)}

        response = request_handler(event)
        self.assertEqual(response["statusCode"], 400)

        body = json.loads(response["body"])
        self.assertIn("error", body)
        self.assertEqual(body["error"], "The queue 'non-existent-queue' does not exist.")

    def test_request_handler_add_request_id(self):
        # Request without a requestId
        request_body = {
            "queueName": self.queue_name,
            "widgetId": "widget123",
            "owner": "test-owner"
        }

        event = {"body": json.dumps(request_body)}

        response = request_handler(event)
        self.assertEqual(response["statusCode"], 200)

        body = json.loads(response["body"])
        self.assertIn("message", body)
        self.assertIn("message_id", body)

    @patch("api.helpers.sqs_client.sqs.send_message", side_effect=Exception("SQS error"))
    def test_request_handler_send_message_failure(self, mock_send_message):
        # Simulate SQS send failure
        request_body = {
            "queueName": self.queue_name,
            "widgetId": "widget123",
            "owner": "test-owner"
        }

        event = {"body": json.dumps(request_body)}

        response = request_handler(event)

        self.assertEqual(response["statusCode"], 500)

        body = json.loads(response["body"])
        self.assertIn("error", body)
        self.assertEqual(body["error"], "Failed to send message to queue.")


if __name__ == "__main__":
    unittest.main()
