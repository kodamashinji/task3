# coding=utf-8

"""
retrieve_request用テストファイル
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
from retrieve_request import list_location_file, get_base_time, separate_location, get_timestamp_and_buffer, \
    remove_location_file, add_partition_to_redshift, get_date_str, compress_and_upload, main
from get_connection_string import get_connection_string


class TestRetrieveRequest(unittest.TestCase):
    """
    TestModule for retrieve_request
    """
    s3, bucket_name, connection_string = (None, None, None)

    @classmethod
    def setUpClass(cls) -> None:
        """
        テスト用バケットを用意するなど
        """
        cls.s3 = boto3.client('s3')
        cls.bucket_name = 'task3test' + str(uuid.uuid4())
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
        cls.s3.create_bucket(Bucket=cls.bucket_name,
                             CreateBucketConfiguration={
                                 'LocationConstraint': 'ap-northeast-1'
                             },
                             ACL='private')

        # 2019/9/1 11:30
        cls.s3.put_object(Bucket=cls.bucket_name, Key='work/1567305370.csv',
                          Body=bytes('3313c918-55e4-4d15-879e-d9fb076a86df,35.7,135.1,1567305370\n', 'ascii'))
        # 1567296000 = 2019/9/1 09:00:00(JST)
        cls.s3.put_object(Bucket=cls.bucket_name, Key='work/1567296001.csv',
                          Body=bytes('3313c918-55e4-4d15-879e-d9fb076a86df,35.7,135.1,1567296001\n'
                                     + '3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567295999\n'
                                     + '3313c918-55e4-4d15-879e-d9fb076a86d1,35.7,135.1,1567296000\n', 'ascii'))
        # 1567263600 = 2019/9/1 00:00:00(JST)
        cls.s3.put_object(Bucket=cls.bucket_name, Key='work/1567263599.csv',
                          Body=bytes('3313c918-55e4-4d15-879e-d9fb076a86df,35.7,135.1,1567263599\n', 'ascii'))
        cls.s3.put_object(Bucket=cls.bucket_name, Key='work/1567263601.csv',
                          Body=bytes('3313c918-55e4-4d15-879e-d9fb076a86df,35.7,135.1,1567263601\n', 'ascii'))
        cls.s3.put_object(Bucket=cls.bucket_name, Key='work/1567263600.csv',
                          Body=bytes('3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567263600\n'
                                     + '3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567263601\n'
                                     + '3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567263599\n', 'ascii'))

        # 1567231200 = 2019/8/31 15:00:00(JST)
        cls.s3.put_object(Bucket=cls.bucket_name, Key='work/1567231200.csv',
                          Body=bytes('3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567231200\n'
                                     + '3313c918-55e4-4d15-879e-d9fb076a86d1,35.7,135.1,1567231200\n', 'ascii'))
        cls.s3.put_object(Bucket=cls.bucket_name, Key='work/1567231199.csv',
                          Body=bytes('3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567231199\n'
                                     + '3313c918-55e4-4d15-879e-d9fb076a86d1,35.7,135.1,1567231199\n', 'ascii'))
        cls.s3.put_object(Bucket=cls.bucket_name, Key='work/1567231201.csv',
                          Body=bytes('3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567231201\n'
                                     + '3313c918-55e4-4d15-879e-d9fb076a86d1,35.7,135.1,1567231201\n', 'ascii'))

        # 1567177200 = 2019/8/31 00:00:00(JST)
        cls.s3.put_object(Bucket=cls.bucket_name, Key='work/1567177201.csv',
                          Body=bytes('3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567177201\n', 'ascii'))
        cls.s3.put_object(Bucket=cls.bucket_name, Key='work/1567177200.csv',
                          Body=bytes('3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567177200\n', 'ascii'))
        cls.s3.put_object(Bucket=cls.bucket_name, Key='work/1567177199.csv',
                          Body=bytes('3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567177199\n', 'ascii'))

    @classmethod
    def tearDownS3(cls) -> None:
        """
        テスト用バケットの後片付け
        """
        request = cls.s3.list_objects_v2(Bucket=cls.bucket_name)
        keys = [c['Key'] for c in request['Contents']]
        while 'NextContinuationToken' in request:
            next_token = request['NextContinuationToken']
            request = cls.s3.list_objects_v2(Bucket=cls.bucket_name, ContinuationToken=next_token)
            keys.extend([c['Key'] for c in request['Contents']])

        cls.s3.delete_objects(Bucket=cls.bucket_name,
                              Delete={'Objects': [{'Key': c} for c in keys if c[-1:] != '/']})
        cls.s3.delete_bucket(Bucket=cls.bucket_name)

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
                  location 's3://{}/parted/' '''.format(cls.bucket_name))

    @classmethod
    def tearDownRedshift(cls) -> None:
        """
        テスト用Redshiftテーブルの削除
        """
        with psycopg2.connect(cls.connection_string) as conn:
            conn.autocommit = True  # CREATE EXTERNAL TABLEはBEGIN内で使えないので、autocommit=Trueにしておく
            with conn.cursor() as cur:
                cur.execute('drop table spectrum.test')

    def test_list_location_file(self) -> None:
        """
        list_location_fileのテスト
        """
        self.assertEqual(
            sorted(list_location_file(bucket=self.bucket_name, prefix='work/')),
            sorted([
                'work/1567305370.csv',
                'work/1567296001.csv',
                'work/1567263599.csv',
                'work/1567263601.csv',
                'work/1567263600.csv',
                'work/1567231200.csv',
                'work/1567231199.csv',
                'work/1567231201.csv',
                'work/1567177201.csv',
                'work/1567177200.csv',
                'work/1567177199.csv'
            ]))

    def test_get_base_time(self) -> None:
        """
        get_base_timeのテスト
        """
        # 1567296000 = 2019/9/1 09:00:00(JST)
        self.assertEqual(get_base_time(1567296000 + 1), 1567263600)
        self.assertEqual(get_base_time(1567296000), 1567263600)
        self.assertEqual(get_base_time(1567296000 - 1), 1567263600)

        # 1567263600 = 2019/9/1 00:00:00(JST)
        self.assertEqual(get_base_time(1567263600 + 1), 1567263600)
        self.assertEqual(get_base_time(1567263600), 1567263600)
        self.assertEqual(get_base_time(1567263600 - 1), 1567177200)

        # 1567231200 = 2019/8/31 15:00:00(JST)
        self.assertEqual(get_base_time(1567231200 + 1), 1567177200)
        self.assertEqual(get_base_time(1567231200), 1567177200)
        self.assertEqual(get_base_time(1567231200 - 1), 1567177200)

    def test_get_date_str(self) -> None:
        """
        get_date_strのテスト
        """
        # 1567296000 = 2019/9/1 09:00:00(JST)
        self.assertEqual(get_date_str(1567296000 + 1), '20190901')
        self.assertEqual(get_date_str(1567296000), '20190901')
        self.assertEqual(get_date_str(1567296000 - 1), '20190901')

        # 1567263600 = 2019/9/1 00:00:00(JST)
        self.assertEqual(get_date_str(1567263600 + 1), '20190901')
        self.assertEqual(get_date_str(1567263600), '20190901')
        self.assertEqual(get_date_str(1567263600 - 1), '20190831')

        # 1567231200 = 2019/8/31 15:00:00(JST)
        self.assertEqual(get_date_str(1567231200 + 1), '20190831')
        self.assertEqual(get_date_str(1567231200), '20190831')
        self.assertEqual(get_date_str(1567231200 - 1), '20190831')

    def test_separate_location(self) -> None:
        """
        separate_locationのテスト
        """
        # 9/1のデータと8/31のデータが混在しているファイル
        result = separate_location('work/1567263600.csv', dict(), bucket=self.bucket_name)
        # 1567177200 = 2019/8/31 00:00:00(JST)

        self.assertIn(1567263600, result)
        self.assertIn(1567177200, result)
        self.assertTrue(os.path.exists(result[1567263600].name))
        self.assertTrue(os.path.exists(result[1567177200].name))
        [os.remove(t.name) for t in result.values()]

    def test_get_timestamp_and_buffer(self) -> None:
        """
        get_timestamp_and_bufferのテスト
        """
        self.assertEqual(
            get_timestamp_and_buffer(b'3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567263600'),
            (1567263600, b'3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567263600\n'))
        self.assertEqual(
            get_timestamp_and_buffer(b'3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567263601\n'),
            (1567263601, b'3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567263601\n'))
        self.assertEqual(get_timestamp_and_buffer(b'3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,hoge\n'), (0, b''))
        self.assertEqual(get_timestamp_and_buffer(b'3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,\n'), (0, b''))
        self.assertEqual(get_timestamp_and_buffer(b'3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1'), (0, b''))
        self.assertEqual(get_timestamp_and_buffer(b'\n'), (0, b''))
        self.assertEqual(get_timestamp_and_buffer(b''), (0, b''))

    def test_remove_location_file(self) -> None:
        """
        remove_location_fileのテスト
        """
        request = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix='work/1567177')
        key_set = set([c['Key'] for c in request['Contents']])
        if 'work/1567177200.csv' not in key_set \
                or 'work/1567177199.csv' not in key_set \
                or 'work/1567177201.csv' not in key_set:
            self.skipTest('test files not exists')

        remove_location_file(['work/1567177200.csv', 'work/1567177199.csv'], bucket=self.bucket_name)

        request = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix='work/1567177')
        key_set = set([c['Key'] for c in request['Contents']])
        self.assertNotIn('work/1567177199.csv', key_set)
        self.assertNotIn('work/1567177200.csv', key_set)
        self.assertIn('work/1567177201.csv', key_set)

    def test_compress_and_upload(self) -> None:
        """
        compress_and_uploadのテスト
        """
        temp = tempfile.NamedTemporaryFile(delete=False)
        try:
            temp.write(b'3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567263601\n')
            temp.close()

            compress_and_upload(temp.name, 'uploaded.csv', '20190826', bucket=self.bucket_name, prefix='parted/')

            r = self.s3.get_object(Bucket=self.bucket_name, Key='parted/created_date=20190826/uploaded.csv.gz')
            self.assertIn('Body', r)
            body = r['Body'].read()
            decompressed = gzip.decompress(body)
            self.assertEqual(decompressed, b'3313c918-55e4-4d15-879e-d9fb076a86d0,35.7,135.1,1567263601\n')
        finally:
            os.remove(temp.name)

    def test_add_partition_to_redshift(self) -> None:
        """
        add_partition_to_redshiftのテスト
        """
        with psycopg2.connect(self.connection_string) as conn:
            conn.autocommit = True  # CREATE EXTERNAL TABLEはBEGIN内で使えないので、autocommit=Trueにしておく
            add_partition_to_redshift(conn, '20190901', 'spectrum.test', self.bucket_name, 'parted/')
            with conn.cursor() as cur:
                cur.execute('select location from svv_external_partitions where tablename=\'test\'')
                results = cur.fetchall()
        self.assertIn('s3://{}/parted/created_date=20190901'.format(self.bucket_name), [x[0] for x in results])

    def test_main(self) -> None:
        """
        mainのテスト
        """
        envs = {'BUCKET_LOCATION': self.bucket_name,
                'FOLDER_WORK': 'work2/',
                'FOLDER_PARTED': 'parted/',
                'TABLE_LOCATION': 'spectrum.test'}

        # 環境変数上書き
        old_values = dict()
        for k, v in envs.items():
            old_values[k] = os.environ.get(k)
            os.environ[k] = v

        # 昨日のファイル2つ、今日のファイル1つ
        now = int(time.time())
        today_base_time = get_base_time(now)
        time1 = str(today_base_time - 23 * 60 * 60 - 59 * 60 - 59)
        time2 = str(today_base_time - 1)
        time3 = str(today_base_time + 1)
        date_prev = get_date_str(today_base_time - 1)
        date_today = get_date_str(today_base_time + 1)

        self.s3.put_object(Bucket=self.bucket_name, Key='work2/' + time1 + '.csv',
                           Body=bytes('3313c918-55e4-4d15-879e-000000000000,35.7,135.1,' + time1 + '\n', 'ascii'))
        self.s3.put_object(Bucket=self.bucket_name, Key='work2/' + time3 + '.csv',
                           Body=bytes('3313c918-55e4-4d15-879e-000000000001,35.7,135.1,' + time3 + '\n', 'ascii'))
        self.s3.put_object(Bucket=self.bucket_name, Key='work2/' + time2 + '.csv',
                           Body=bytes('3313c918-55e4-4d15-879e-000000000002,35.7,135.1,' + time1 + '\n'
                                      + '3313c918-55e4-4d15-879e-000000000003,35.7,135.1,' + time2 + '\n'
                                      + '3313c918-55e4-4d15-879e-000000000004,35.7,135.1,' + time3 + '\n', 'ascii'))

        # call main()
        self.assertEqual(main(), 'success')

        request = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix='work2/')
        self.assertEqual(request['KeyCount'], 1)  # 本日分データが戻されているはず

        request = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix='parted/created_date=' + date_prev + '/')
        self.assertGreaterEqual(request['KeyCount'], 1)  # 昨日分データが作成されている

        with psycopg2.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute('select user_id from spectrum.test where created_date=\'' + date_prev + '\'')
                results_prev = cur.fetchall()

                cur.execute('select user_id from spectrum.test where created_date=\'' + date_today + '\'')
                results_today = cur.fetchall()

                for user_id in ['3313c918-55e4-4d15-879e-000000000000',
                                '3313c918-55e4-4d15-879e-000000000002',
                                '3313c918-55e4-4d15-879e-000000000003']:
                    self.assertIn((user_id,), results_prev)
                    self.assertNotIn((user_id,), results_today)

                for user_id in ['3313c918-55e4-4d15-879e-000000000001',
                                '3313c918-55e4-4d15-879e-000000000004']:
                    self.assertNotIn((user_id,), results_prev)
                    self.assertNotIn((user_id,), results_today)

        self.s3.delete_objects(Bucket=self.bucket_name,
                               Delete={'Objects': [{'Key': 'work2/' + c + '.csv'} for c in [time1, time2, time3]]})

        # 環境変数戻す
        for k in envs.keys():
            if old_values[k] is None:
                del os.environ[k]
            else:
                os.environ[k] = old_values[k]
