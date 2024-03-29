# coding=utf-8

"""
S3作業用フォルダ(s3://..../work)にあるオブジェクトを読み込み、日付毎に振り分ける。
振り分けた結果は、S3格納用フォルダ(s3://..../parted)に書き出される。
ただし、このプログラムを実行した当日のデータは一時保管用フォルダに戻されて、翌日以降に書き出される。

なお、全体の処理手順は以下の通り
parse_request  ->  store_request  ->  retrieve_request  -> [collect_request]
"""

import boto3
import logging
import time
import datetime
import tempfile
import sys
import gzip
import os
import psycopg2
import psycopg2.extensions
from typing import BinaryIO
from get_connection_string import get_connection_string

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_BUCKET_DOWNLOAD = 'me32as8cme32as8c-task3-download'
DEFAULT_TABLE_LOCATION = 'spectrum.location'

s3 = boto3.client('s3')
tz = 9 * 60 * 60   # JST(+9:00)


def select_and_write_location(ymd: str, result_file: BinaryIO, table: str) -> int:
    """
    Redshiftから該当日のデータを取得し、一時ファイルに書き出す

    Parameters
    ----------
    ymd:str
        該当日となる年月日 (YYYYMMDD)
    result_file: BinaryIO
        一時ファイルのio
    table: str
        Redshift内のテーブル名

    Returns
    -------
    int
        書き込んだレコード数
    """
    records = 0
    connection_string = get_connection_string()
    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute('select user_id, latitude, longitude, created_at from {} where created_date=\'{}\''
                           .format(table, ymd))
            for row in cursor:
                result_file.write(bytes(','.join([str(x) for x in row]) + '\r\n', 'ascii'))
                records = records + 1

    return records


def main(ymd: str) -> str:
    """
    メイン
    Parameters
    ----------
    ymd: str
        該当日となる年月日 (YYYYMMDD)

    Returns
    -------
    str:
        "success" or "error"
    """
    bucket = os.environ.get('BUCKET_DOWNLOAD', DEFAULT_BUCKET_DOWNLOAD)
    table_location = os.environ.get('TABLE_LOCATION', DEFAULT_TABLE_LOCATION)

    try:
        logger.info('start.')

        with tempfile.TemporaryFile() as ftemp:
            # Redshiftからqueryし、一時ファイルに圧縮して書き出す
            with gzip.GzipFile(fileobj=ftemp, mode='w+b') as fout:
                records = select_and_write_location(ymd, fout, table_location)

            # 空ファイルで無ければ、一時ファイルをS3にアップロードする
            if records > 0:
                ftemp.seek(0)
                s3.upload_fileobj(Fileobj=ftemp, Bucket=bucket, Key=ymd + '.csv.gz')

        logger.info('finished.')
        return 'success'
    except Exception as e:
        logger.error(e)

    return 'error'


if __name__ == "__main__":
    if len(sys.argv) == 1:  # 引数なしの場合は昨日のデータ
        local_time = time.time() + tz
        local_yesterday_base_time = local_time - (local_time % (60 * 60 * 24)) - (60 * 60 * 24)
        ymd_str = datetime.datetime.utcfromtimestamp(local_yesterday_base_time).strftime('%Y%m%d')  # YYYYMMDD
    else:
        ymd_str = sys.argv[1]

    main(ymd_str)
