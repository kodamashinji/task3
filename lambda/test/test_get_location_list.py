# coding=utf-8

"""
get_location_list用テストファイル
"""

import unittest
import boto3
import uuid
import warnings
import requests
import botocore.exceptions

import sys
sys.path.append('..')
from get_location_list import get_key, check_file, get_presigned_url


class TestGetLocationList(unittest.TestCase):
    """
    TestModule for get_location_list
    """
    s3, bucket_name = (None, None)

    @classmethod
    def setUpClass(cls) -> None:
        """
        SQSクライアントを作成し、SQSにテスト用のキューを作成する
        S3クライアントを作成し、テスト用バケットを用意する
        """
        cls.s3 = boto3.client('s3')
        cls.bucket_name = 'task3test' + str(uuid.uuid4())
        cls.s3.create_bucket(Bucket=cls.bucket_name,
                             CreateBucketConfiguration={
                                 'LocationConstraint': 'ap-northeast-1'
                             },
                             ACL='private')
        cls.s3.put_object(Bucket=cls.bucket_name, Key='20190901.csv.gz', Body=b'dummy')

        # BOTO3かunittestの不具合避け
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

    @classmethod
    def tearDownClass(cls) -> None:
        """
        作成したキューとバケットの後片付け
        """
        cls.s3.delete_object(Bucket=cls.bucket_name, Key='20190901.csv.gz')
        cls.s3.delete_bucket(Bucket=cls.bucket_name)

    def test_get_key(self) -> None:
        """
        get_keyのテスト
        """
        self.assertRaises(KeyError, get_key, {})
        self.assertEqual(get_key({'ymd': '20190901'}), '20190901.csv.gz')

    def test_check_file(self) -> None:
        """
        check_fileのテスト
        """
        self.assertRaises(botocore.exceptions.ClientError, check_file, 'no_suck_key')
        self.assertRaises(botocore.exceptions.ParamValidationError, check_file, '')
        self.assertIsNone(check_file('20190901.csv.gz', bucket=self.bucket_name))

    def test_get_presigned_url(self) -> None:
        """
        get_presigned_urlのテスト
        """
        url = get_presigned_url('20190901.csv.gz', bucket=self.bucket_name)
        r = requests.get(url)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.content, b'dummy')
