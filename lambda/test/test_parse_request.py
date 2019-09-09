# coding=utf-8

"""
parse_request用テストファイル
"""

import unittest
import boto3
import uuid
import warnings
import os

import sys
sys.path.append('..')
from parse_request import parse_request, push_location, lambda_handler


class TestParseRequest(unittest.TestCase):
    """
    TestModule for parse_request
    """
    sqs, queue_url, queue_name = (None, None, None)

    @classmethod
    def setUpClass(cls) -> None:
        """
        SQSクライアントを作成し、SQSにテスト用のキューを作成する
        """
        cls.sqs = boto3.client('sqs')
        cls.queue_name = 'task3test' + str(uuid.uuid4())
        response = cls.sqs.create_queue(QueueName=cls.queue_name)
        cls.queue_url = response['QueueUrl']
        # BOTO3かunittestの不具合避け
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

    @classmethod
    def tearDownClass(cls) -> None:
        """
        作成したキューの後片付け
        """
        cls.sqs.delete_queue(QueueUrl=cls.queue_url)

    def test_parse_request(self) -> None:
        """
        parse_request関数のテスト
        """
        self.assertRaises(KeyError, parse_request, {})
        self.assertRaises(TypeError, parse_request, '')
        self.assertRaises(ValueError, parse_request, {
            'user_id': 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5',
            'location': {
                'lat_north_south': '?',            # INVALID
                'latitude': '35.744947',
                'lon_west_east': 'E',
                'longitude': '139.720168'
            },
            'timestamp': 1555055157
        })
        self.assertRaises(ValueError, parse_request, {
            'user_id': 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5',
            'location': {
                'lat_north_south': 'N',
                'latitude': '35.744947',
                'lon_west_east': '?',              # INVALID
                'longitude': '139.720168'
            },
            'timestamp': 1555055157
        })
        self.assertRaises(ValueError, parse_request, {
            'user_id': 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5',
            'location': {
                'lat_north_south': 'N',
                'latitude': 'x',                   # INVALID
                'lon_west_east': 'E',
                'longitude': '139.720168'
            },
            'timestamp': 1555055157
        })
        self.assertRaises(ValueError, parse_request, {
            'user_id': 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5',
            'location': {
                'lat_north_south': 'N',
                'latitude': '35.744947',
                'lon_west_east': 'E',
                'longitude': 'x'                   # INVALID
            },
            'timestamp': 1555055157
        })
        self.assertRaises(ValueError, parse_request, {
            'user_id': 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5',
            'location': {
                'lat_north_south': 'N',
                'latitude': '35.744947',
                'lon_west_east': 'E',
                'longitude': '139.720168'
            },
            'timestamp': 'x'                       # INVALID
        })
        self.assertEqual(parse_request({           # 北緯東経
            'user_id': 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5',
            'location': {
                'lat_north_south': 'N',
                'latitude': '35.744947',
                'lon_west_east': 'E',
                'longitude': '139.720168'
            },
            'timestamp': 1555055157
        }), 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5,35.744947,139.720168,1555055157')
        self.assertEqual(parse_request({           # 南緯東経
            'user_id': 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5',
            'location': {
                'lat_north_south': 'S',
                'latitude': '35.744947',
                'lon_west_east': 'E',
                'longitude': '139.720168'
            },
            'timestamp': 1555055157
        }), 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5,-35.744947,139.720168,1555055157')
        self.assertEqual(parse_request({           # 北緯西経
            'user_id': 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5',
            'location': {
                'lat_north_south': 'N',
                'latitude': '35.744947',
                'lon_west_east': 'W',
                'longitude': '139.720168'
            },
            'timestamp': 1555055157
        }), 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5,35.744947,-139.720168,1555055157')
        self.assertEqual(parse_request({           # 南緯西経
            'user_id': 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5',
            'location': {
                'lat_north_south': 'S',
                'latitude': '35.744947',
                'lon_west_east': 'W',
                'longitude': '139.720168'
            },
            'timestamp': 1555055157
        }), 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5,-35.744947,-139.720168,1555055157')

    def test_push_location(self) -> None:
        """
        push_location関数のテスト
        """
        location = 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5,35.744947,139.720168,1555055157'
        push_location(location, self.queue_name)

        messages = self.sqs.receive_message(QueueUrl=self.queue_url)
        self.assertNotEqual([x['Body'] for x in messages['Messages'] if x['Body'] == location], [])

    def test_lambda_handler(self) -> None:
        """
        lambda_handler関数のテスト
        """
        old_queue_name = os.environ.get('QUEUE_NAME')
        os.environ['QUEUE_NAME'] = self.queue_name

        lambda_handler({
            'user_id': 'cf5bff5c-2ffe-4f18-9593-bc666313f8c6',
            'location': {
                'lat_north_south': 'N',
                'latitude': '35.744947',
                'lon_west_east': 'E',
                'longitude': '139.720168'
            },
            'timestamp': 1555055157
        }, None)

        location = 'cf5bff5c-2ffe-4f18-9593-bc666313f8c6,35.744947,139.720168,1555055157'
        messages = self.sqs.receive_message(QueueUrl=self.queue_url)
        self.assertNotEqual([x['Body'] for x in messages['Messages'] if x['Body'] == location], [])

        if old_queue_name is None:
            del os.environ['QUEUE_NAME']
        else:
            os.environ['QUEUE_NAME'] = old_queue_name
