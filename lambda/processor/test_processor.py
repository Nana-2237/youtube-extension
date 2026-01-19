import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from processor import process_record, lambda_handler
from aggregator import aggregate_ndjson


class TestProcessor(unittest.TestCase):
    def test_aggregate_ndjson(self):
        ndjson = """{"event_type":"video_start","event_ts":1000,"tab_id":"t1","video_id":"v1","channel_name":"Channel1","video_session_id":"s1"}
{"event_type":"watch_tick","event_ts":2000,"tab_id":"t1","video_id":"v1","channel_name":"Channel1","watch_ms_delta":5000,"video_session_id":"s1","watch_mode":"foreground"}
{"event_type":"watch_tick","event_ts":3000,"tab_id":"t1","video_id":"v1","channel_name":"Channel1","watch_ms_delta":3000,"video_session_id":"s1","watch_mode":"background"}"""
        
        result = aggregate_ndjson(ndjson)
        
        self.assertEqual(result['views']['views_by_video']['v1'], 1)
        self.assertEqual(result['views']['views_by_channel']['Channel1'], 1)
        self.assertEqual(result['totals']['total_ms_by_video']['v1'], 8000)
        self.assertEqual(result['totals']['total_ms_by_channel']['Channel1'], 8000)
        self.assertEqual(result['totals']['total_ms_by_channel_fg']['Channel1'], 5000)
        self.assertEqual(result['totals']['total_ms_by_channel_bg']['Channel1'], 3000)

    @patch('s3_operations.s3')
    @patch('processor.s3')
    def test_process_record(self, mock_processor_s3, mock_ops_s3):
        mock_s3 = mock_processor_s3
        mock_s3.head_object.return_value = {"ETag": '"test-etag"'}
        body_mock = MagicMock()
        body_mock.read.return_value = b'{"event_type":"video_start","event_ts":1000,"tab_id":"t1","video_id":"v1","channel_name":"ch1","video_session_id":"s1"}'
        mock_s3.get_object.return_value = {"Body": body_mock}
        mock_ops_s3.put_object.return_value = {}
        mock_ops_s3.copy_object.return_value = {}
        mock_ops_s3.delete_object.return_value = {}
        
        rec = {
            "s3": {
                "bucket": {"name": "test-bucket"},
                "object": {"key": "raw/2026/01/19/test-file.json"}
            }
        }
        
        result = process_record(rec)
        self.assertTrue(result)

    def test_process_record_invalid_key(self):
        rec = {
            "s3": {
                "bucket": {"name": "test-bucket"},
                "object": {"key": "other/prefix/file.json"}
            }
        }
        
        result = process_record(rec)
        self.assertFalse(result)

    @patch('processor.process_record')
    def test_lambda_handler(self, mock_process):
        mock_process.return_value = True
        
        event = {
            "Records": [
                {"s3": {"bucket": {"name": "b1"}, "object": {"key": "raw/2026/01/19/f1.json"}}},
                {"s3": {"bucket": {"name": "b1"}, "object": {"key": "raw/2026/01/19/f2.json"}}}
            ]
        }
        
        result = lambda_handler(event, None)
        self.assertEqual(result['processed'], 2)
        self.assertEqual(mock_process.call_count, 2)


if __name__ == '__main__':
    unittest.main()

