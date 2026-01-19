import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from routes import register_routes
from flask import Flask


class TestAPIServer(unittest.TestCase):
    @patch('config.validate_config')
    def setUp(self, mock_validate):
        self.app = Flask(__name__)
        register_routes(self.app)
        self.client = self.app.test_client()

    @patch('firehose_client.send_batch')
    @patch('storage.append_local_ndjson')
    def test_ingest_valid_event(self, mock_append, mock_send_batch):
        event = {
            "schema": 1,
            "event_id": "test-123",
            "event_ts": 1234567890,
            "event_type": "video_start",
            "client_session_id": "test-session",
            "tab_id": "test-tab",
            "video_id": "test-video-123",
            "video_session_id": "test-video-session"
        }
        
        response = self.client.post('/ingest', 
                                   json={"events": [event]},
                                   content_type='application/json')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['accepted'], 1)
        self.assertEqual(data['rejected'], 0)

    def test_ingest_invalid_json(self):
        response = self.client.post('/ingest',
                                   data='invalid json',
                                   content_type='application/json')
        self.assertEqual(response.status_code, 400)

    def test_ingest_invalid_event(self):
        invalid_event = {"invalid": "event"}
        response = self.client.post('/ingest',
                                   json={"events": [invalid_event]},
                                   content_type='application/json')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['accepted'], 0)
        self.assertGreater(data['rejected'], 0)

    def test_health_endpoint(self):
        response = self.client.get('/health')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertTrue(data['ok'])

    def test_ingest_options(self):
        response = self.client.options('/ingest')
        self.assertEqual(response.status_code, 204)


if __name__ == '__main__':
    unittest.main()

