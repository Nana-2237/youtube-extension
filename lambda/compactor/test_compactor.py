import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from aggregator import merge_dict_add, aggregate_partials, build_rows
from utils import dt_today_utc, prefix_for_partials, prefix_for_out


class TestCompactor(unittest.TestCase):
    def test_merge_dict_add(self):
        dst = {"a": 10, "b": 20}
        src = {"a": 5, "b": 15, "c": 30}
        merge_dict_add(dst, src)
        self.assertEqual(dst["a"], 15)
        self.assertEqual(dst["b"], 35)
        self.assertEqual(dst["c"], 30)

    def test_merge_dict_add_ignores_non_numeric(self):
        dst = {"a": 10}
        src = {"a": 5, "b": "string", "c": None}
        merge_dict_add(dst, src)
        self.assertEqual(dst["a"], 15)
        self.assertNotIn("b", dst)
        self.assertNotIn("c", dst)

    @patch('s3_operations.read_json')
    def test_aggregate_partials(self, mock_read):
        mock_read.side_effect = [
            {
                "totals": {
                    "total_ms_by_channel": {"ch1": 1000, "ch2": 2000},
                    "total_ms_by_channel_fg": {"ch1": 800, "ch2": 1500},
                    "total_ms_by_channel_bg": {"ch1": 200, "ch2": 500},
                    "total_ms_by_video": {"v1": 1000, "v2": 2000}
                },
                "views": {
                    "views_by_channel": {"ch1": 5, "ch2": 10},
                    "views_by_video": {"v1": 5, "v2": 10}
                }
            },
            {
                "totals": {
                    "total_ms_by_channel": {"ch1": 500},
                    "total_ms_by_channel_fg": {"ch1": 400},
                    "total_ms_by_channel_bg": {"ch1": 100},
                    "total_ms_by_video": {"v1": 500}
                },
                "views": {
                    "views_by_channel": {"ch1": 2},
                    "views_by_video": {"v1": 2}
                }
            }
        ]
        
        result = aggregate_partials(["key1", "key2"], "test-bucket")
        
        self.assertEqual(result["channels"]["ch1"], 1500)
        self.assertEqual(result["channels"]["ch2"], 2000)
        self.assertEqual(result["views_channels"]["ch1"], 7)
        self.assertEqual(result["views_channels"]["ch2"], 10)

    def test_build_rows(self):
        aggregated = {
            "channels": {"ch1": 1000, "ch2": 2000},
            "channels_fg": {"ch1": 800, "ch2": 1500},
            "channels_bg": {"ch1": 200, "ch2": 500},
            "videos": {"v1": 1000, "v2": 2000},
            "views_channels": {"ch1": 5, "ch2": 10},
            "views_videos": {"v1": 5, "v2": 10}
        }
        
        channel_rows, video_rows = build_rows(aggregated, "2026-01-19")
        
        self.assertEqual(len(channel_rows), 2)
        self.assertEqual(len(video_rows), 2)
        self.assertEqual(channel_rows[0]["channel"], "ch1")
        self.assertEqual(channel_rows[0]["watch_ms"], 1000)
        self.assertEqual(video_rows[0]["video_id"], "v1")
        self.assertEqual(video_rows[0]["watch_ms"], 1000)

    def test_prefix_for_partials(self):
        result = prefix_for_partials("2026-01-19")
        self.assertEqual(result, "results/daily/2026/01/19/partials/")

    def test_prefix_for_out(self):
        result = prefix_for_out("channel_daily", "2026-01-19")
        self.assertEqual(result, "analytics/channel_daily/dt=2026-01-19/")


if __name__ == '__main__':
    unittest.main()

