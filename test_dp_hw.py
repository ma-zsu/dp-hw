import unittest
from io import StringIO
from unittest.mock import patch
import csv
from datetime import datetime
from dp_hw import parse_event_time, calculate_session_length, process_csv

class TestCSVProcessing(unittest.TestCase):
    
    def test_parse_event_time(self):
        event_time = "2024-02-15T14:30:00.000Z"
        expected = datetime(2024, 2, 15, 14, 30)
        self.assertEqual(parse_event_time(event_time), expected)
    
    def test_calculate_session_length(self):
        in_time = datetime(2024, 2, 15, 14, 30)
        out_time = datetime(2024, 2, 15, 16, 30)
        expected = 2  # 2 hours
        self.assertEqual(calculate_session_length(in_time, out_time), expected)
    
    @patch('builtins.open')
    def test_process_csv(self, mock_open):
        # Mock CSV data
        mock_csv_data = """user_id,event_time,event_type
                          1,2024-02-15T14:30:00.000Z,gate_in
                          1,2024-02-15T16:30:00.000Z,gate_out
                          2,2024-02-16T10:00:00.000Z,gate_in
                          2,2024-02-16T12:00:00.000Z,gate_out"""
        mock_open.return_value = StringIO(mock_csv_data)
        
        # Process CSV data
        ranked_users, sorted_sessions = process_csv('input/dp_hw.csv')
        
        # Expected results
        expected_ranked_users = [
            ('1', 2.0, 1, 2.0, 1),
            ('2', 2.0, 1, 2.0, 2)
        ]
        expected_sorted_sessions = [
            ('1', 2.0),
            ('2', 2.0)
        ]
        
        self.assertEqual(ranked_users, expected_ranked_users)
        self.assertEqual(sorted_sessions, expected_sorted_sessions)

if __name__ == "__main__":
    unittest.main()