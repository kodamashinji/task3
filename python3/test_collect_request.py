# coding=utf-8

"""
collect_request用テストファイル
"""

import unittest
import boto3
import uuid
import warnings
from collect_request import redshift_connect, get_pgpass, select_and_write_location


class TestCollectRequest(unittest.TestCase):
    """
    TestModule for collect_request
    """
    # TODO
    # TODO


    sqs = None
    queue_url = None
    queue_name = 'test' + str(uuid.uuid4())

    @classmethod
    def setUpClass(cls) -> None:
        """
        SQSクライアントを作成し、SQSにテスト用のキューを作成する
        """
        cls.sqs = boto3.client('sqs')
        response = cls.sqs.create_queue(
            QueueName=cls.queue_name
        )
        cls.queue_url = response['QueueUrl']
        # BOTO3かunittestの不具合避け
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

    @classmethod
    def tearDownClass(cls) -> None:
        """
        作成したキューの後片付け
        """
        cls.sqs.delete_queue(
            QueueUrl=cls.queue_url
        )

