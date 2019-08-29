# coding=utf-8

"""
parse_request用テストファイル
"""

import unittest
import boto3
import uuid
import warnings
from retrieve_request import list_location_file, get_base_time, separate_location, get_timestamp_and_buffer,\
    remove_location_file, redshift_connect, get_pgpass, add_partition_to_redshift


class TestRetrieveRequest(unittest.TestCase):
    """
    TestModule for retrieve_request
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

