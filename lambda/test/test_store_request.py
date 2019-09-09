# coding=utf-8

"""
store_request用テストファイル
"""

import unittest
import boto3
import uuid
import warnings
import os
from typing import List

import sys
sys.path.append('..')
from store_request import retrieve_location, write_location, lambda_handler


class TestStoreRequest(unittest.TestCase):
    """
    TestModule for store_request
    """
    sqs, queue_url, queue_name = (None, None, None)
    s3, bucket_name = (None, None)

    @classmethod
    def setUpClass(cls) -> None:
        """
        SQSクライアントを作成し、SQSにテスト用のキューを作成する
        S3クライアントを作成し、テスト用バケットを用意する
        """
        cls.sqs = boto3.client('sqs')
        cls.queue_name = 'task3test' + str(uuid.uuid4())
        response = cls.sqs.create_queue(QueueName=cls.queue_name)
        cls.queue_url = response['QueueUrl']

        cls.s3 = boto3.client('s3')
        cls.bucket_name = 'task3test' + str(uuid.uuid4())
        cls.s3.create_bucket(Bucket=cls.bucket_name,
                             CreateBucketConfiguration={
                                 'LocationConstraint': 'ap-northeast-1'
                             },
                             ACL='private')

        # BOTO3かunittestの不具合避け
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

    @classmethod
    def tearDownClass(cls) -> None:
        """
        作成したキューとバケットの後片付け
        """
        cls.sqs.delete_queue(QueueUrl=cls.queue_url)

        cls.s3.delete_object(Bucket=cls.bucket_name, Key='work/test.csv')
        cls.s3.delete_bucket(Bucket=cls.bucket_name)

    def test_retrieve_location(self) -> None:
        """
        retrieve_locationのテスト
        """
        # 空リスト
        self.assertEqual(retrieve_location(self.queue_name), [])

        # ダミーデータ
        for location in self.dummyLocationList():
            self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=location)

        # キューへの投入順序は保証されないのでsetで比較する
        self.assertEqual(set(retrieve_location(self.queue_name)), set(self.dummyLocationList()))

        # すべて取得した後なので空になっているはず
        self.assertEqual(retrieve_location(self.queue_name), [])

    def test_write_location(self) -> None:
        """
        write_locationのテスト
        """
        # ダミーデータの書き込み
        file_name = 'test.csv'
        folder = 'work/'
        write_location(file_name, sorted(list(set(self.dummyLocationList()))), bucket=self.bucket_name, prefix=folder)

        result = self.s3.get_object(Bucket=self.bucket_name, Key=folder + file_name)
        body = result['Body'].read()
        self.maxDiff = None
        self.assertEqual(body.decode('ascii'), self.dummyObjectBody())

    def test_lambda_handler(self) -> None:
        """
        lambda_handler関数のテスト
        """
        old_queue_name = os.environ.get('QUEUE_NAME')
        old_bucket = os.environ.get('BUCKET_LOCATION')
        old_folder = os.environ.get('FOLDER_WORK')

        os.environ['QUEUE_NAME'] = self.queue_name
        os.environ['BUCKET_LOCATION'] = self.bucket_name
        os.environ['FOLDER_WORK'] = 'work/'

        # ダミーデータ
        for location in self.dummyLocationList():
            self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=location)

        lambda_handler({}, None)

        list_object = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix='work/')
        self.assertIn('Contents', list_object)
        self.assertEqual(len(list_object['Contents']), 1)
        key = list_object['Contents'][0]['Key']

        get_object = self.s3.get_object(Bucket=self.bucket_name, Key=key)
        self.assertIn('Body', get_object)
        body = get_object['Body'].read()

        self.assertEqual(sorted(body.decode('ascii').split('\n')), sorted(self.dummyObjectBody().split('\n')))

        self.s3.delete_object(Bucket=self.bucket_name, Key=key)

        if old_queue_name is None:
            del os.environ['QUEUE_NAME']
        else:
            os.environ['QUEUE_NAME'] = old_queue_name

        if old_bucket is None:
            del os.environ['WORK_BUCKET']
        else:
            os.environ['WORK_BUCKET'] = old_bucket

        if old_folder is None:
            del os.environ['WORK_FOLDER']
        else:
            os.environ['WORK_FOLDER'] = old_folder

    @staticmethod
    def dummyLocationList() -> List[str]:
        """
        ダミー位置情報のデータ
        15件あるが、重複が2件あり、unique件数は13件

        Returns
        -------
        str
        """
        return [
            'cf5bff5c-2ffe-4f18-9593-bc666313f800,35.744947,139.720168,1555055100',
            'cf5bff5c-2ffe-4f18-9593-bc666313f800,35.744947,139.720168,1555055101',
            'cf5bff5c-2ffe-4f18-9593-bc666313f801,35.744947,139.720168,1555055100',
            'cf5bff5c-2ffe-4f18-9593-bc666313f802,35.744947,139.720168,1555055100',
            'cf5bff5c-2ffe-4f18-9593-bc666313f803,35.744947,139.720168,1555055100',
            'cf5bff5c-2ffe-4f18-9593-bc666313f804,35.744947,139.720168,1555055100',
            'cf5bff5c-2ffe-4f18-9593-bc666313f802,35.744947,139.720168,1555055101',
            'cf5bff5c-2ffe-4f18-9593-bc666313f807,35.744947,139.720168,1555055100',
            'cf5bff5c-2ffe-4f18-9593-bc666313f806,35.744947,139.720168,1555055100',
            'cf5bff5c-2ffe-4f18-9593-bc666313f805,35.744947,139.720168,1555055100',
            'cf5bff5c-2ffe-4f18-9593-bc666313f808,35.744947,139.720168,1555055100',
            'cf5bff5c-2ffe-4f18-9593-bc666313f802,35.744947,139.720168,1555055100',
            'cf5bff5c-2ffe-4f18-9593-bc666313f800,35.744947,139.720168,1555055100',
            'cf5bff5c-2ffe-4f18-9593-bc666313f809,35.744947,139.720168,1555055101',
            'cf5bff5c-2ffe-4f18-9593-bc666313f809,35.744947,139.720168,1555055100'
        ]

    @staticmethod
    def dummyObjectBody() -> str:
        """
        ファイルに保存されている(はず)のデータ
        "\n".join(sorted(list(set(dummyLocationList)))) + "\n"

        Returns
        -------
        str
        """
        return """cf5bff5c-2ffe-4f18-9593-bc666313f800,35.744947,139.720168,1555055100
cf5bff5c-2ffe-4f18-9593-bc666313f800,35.744947,139.720168,1555055101
cf5bff5c-2ffe-4f18-9593-bc666313f801,35.744947,139.720168,1555055100
cf5bff5c-2ffe-4f18-9593-bc666313f802,35.744947,139.720168,1555055100
cf5bff5c-2ffe-4f18-9593-bc666313f802,35.744947,139.720168,1555055101
cf5bff5c-2ffe-4f18-9593-bc666313f803,35.744947,139.720168,1555055100
cf5bff5c-2ffe-4f18-9593-bc666313f804,35.744947,139.720168,1555055100
cf5bff5c-2ffe-4f18-9593-bc666313f805,35.744947,139.720168,1555055100
cf5bff5c-2ffe-4f18-9593-bc666313f806,35.744947,139.720168,1555055100
cf5bff5c-2ffe-4f18-9593-bc666313f807,35.744947,139.720168,1555055100
cf5bff5c-2ffe-4f18-9593-bc666313f808,35.744947,139.720168,1555055100
cf5bff5c-2ffe-4f18-9593-bc666313f809,35.744947,139.720168,1555055100
cf5bff5c-2ffe-4f18-9593-bc666313f809,35.744947,139.720168,1555055101
"""
