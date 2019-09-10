# coding=utf-8

"""
collect_request用テストファイル
"""

import unittest
import boto3
import uuid
import os
import warnings
import tempfile
import gzip
import psycopg2
import time

import sys

sys.path.append('..')
from collect_request import select_and_write_location, main
from get_connection_string import get_connection_string


class TestCollectRequest(unittest.TestCase):
    """
    TestModule for collect_request
    """
    """
    TestModule for retrieve_request
    """
    s3, location_bucket_name, download_bucket_name, connection_string = (None, None, None, None)

    @classmethod
    def setUpClass(cls) -> None:
        """
        テスト用バケットを用意するなど
        """
        cls.s3 = boto3.client('s3')
        cls.location_bucket_name = 'task3test' + str(uuid.uuid4())
        cls.download_bucket_name = 'task3test' + str(uuid.uuid4())
        cls.connection_string = get_connection_string()

        cls.setUpS3()
        cls.setUpRedshift()

        # BOTO3かunittestの不具合避け
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

    @classmethod
    def tearDownClass(cls) -> None:
        """
        後片付け
        """
        cls.tearDownS3()
        cls.tearDownRedshift()

    @classmethod
    def setUpS3(cls) -> None:
        """
        テスト用バケットを用意する
        """
        cls.s3.create_bucket(Bucket=cls.location_bucket_name,
                             CreateBucketConfiguration={
                                 'LocationConstraint': 'ap-northeast-1'
                             },
                             ACL='private')
        cls.s3.create_bucket(Bucket=cls.download_bucket_name,
                             CreateBucketConfiguration={
                                 'LocationConstraint': 'ap-northeast-1'
                             },
                             ACL='private')

        # 1567263600 = 2019/9/1 00:00:00(JST)
        body = bytes('3313c918-55e4-4d15-879e-000000000000,35.71,135.11,1567273600\n'
                     + '3313c918-55e4-4d15-879e-000000000001,35.72,135.12,1567273601\n'
                     + '3313c918-55e4-4d15-879e-000000000002,35.73,135.13,1567273602\n', 'ascii')
        body_zipped = gzip.compress(body)

        cls.s3.put_object(Bucket=cls.location_bucket_name,
                          Key='parted/created_date=20190901/test.csv.gz',
                          Body=body_zipped)

    @classmethod
    def tearDownS3(cls) -> None:
        """
        テスト用バケットの後片付け
        """
        cls.s3.delete_object(Bucket=cls.location_bucket_name, Key='parted/created_date=20190901/test.csv.gz')
        cls.s3.delete_bucket(Bucket=cls.location_bucket_name)
        cls.s3.delete_object(Bucket=cls.download_bucket_name, Key='20190901.csv.gz')
        cls.s3.delete_bucket(Bucket=cls.download_bucket_name)

    @classmethod
    def setUpRedshift(cls) -> None:
        """
        テスト用Redshiftテーブルの作成
        """
        with psycopg2.connect(cls.connection_string) as conn:
            conn.autocommit = True  # CREATE EXTERNAL TABLEはBEGIN内で使えないので、autocommit=Trueにしておく
            with conn.cursor() as cur:
                cur.execute('''create external table spectrum.test(
                  user_id char(36),
                  latitude double precision,
                  longitude double precision,
                  created_at integer)
                  partitioned by (created_date char(8))
                  row format delimited fields terminated by ',' stored as textfile
                  location 's3://{}/parted/' '''.format(cls.location_bucket_name))
                cur.execute('alter table spectrum.test add if not exists partition(created_date=\'20190901\')'
                            + ' location \'s3://{}/parted/created_date=20190901/\' '.format(cls.location_bucket_name))

    @classmethod
    def tearDownRedshift(cls) -> None:
        """
        テスト用Redshiftテーブルの削除
        """
        with psycopg2.connect(cls.connection_string) as conn:
            conn.autocommit = True  # CREATE EXTERNAL TABLEはBEGIN内で使えないので、autocommit=Trueにしておく
            with conn.cursor() as cur:
                cur.execute('drop table spectrum.test')

    def test_select_and_location(self) -> None:
        """
        select_and_locationのテスト
        """
        with tempfile.TemporaryFile() as ftemp:
            records = select_and_write_location('20190901', ftemp, 'spectrum.test')
            self.assertEqual(records, 3)

            ftemp.seek(0)

            self.maxDiff = None
            self.assertEqual(ftemp.read(),
                             bytes('3313c918-55e4-4d15-879e-000000000000,35.71,135.11,1567273600\r\n'
                                   + '3313c918-55e4-4d15-879e-000000000001,35.72,135.12,1567273601\r\n'
                                   + '3313c918-55e4-4d15-879e-000000000002,35.73,135.13,1567273602\r\n', 'ascii'))

    def test_main(self) -> None:
        """"
        mainのテスト
        """
        envs = {'BUCKET_DOWNLOAD': self.download_bucket_name,
                'TABLE_LOCATION': 'spectrum.test'}

        # 環境変数上書き
        old_values = dict()
        for k, v in envs.items():
            old_values[k] = os.environ.get(k)
            os.environ[k] = v

        # call main() #1
        self.assertEqual(main('20190901'), 'success')
        result = self.s3.list_objects_v2(Bucket=self.download_bucket_name, Prefix='20190901.csv.gz')
        self.assertEqual(result['KeyCount'], 1)

        # call main() #2 (no data)
        self.assertEqual(main('20190831'), 'success')
        result = self.s3.list_objects_v2(Bucket=self.download_bucket_name, Prefix='20190831.csv.gz')
        self.assertEqual(result['KeyCount'], 0)

        # 環境変数戻す
        for k in envs.keys():
            if old_values[k] is None:
                del os.environ[k]
            else:
                os.environ[k] = old_values[k]
