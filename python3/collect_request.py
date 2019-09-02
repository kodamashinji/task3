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
import os
import tempfile
import sys
import gzip
import shutil
import psycopg2
import psycopg2.extensions
from typing import Type, BinaryIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_NAME = 'request-queue'
DOWNLOAD_BUCKET = 'me32as8cme32as8c-task3-download'

s3 = boto3.client('s3')
tz = 9 * 60 * 60   # JST(+9:00)


def get_connection_string(config_file: str = None) -> str:
    """
    Redshiftへのコネクション設定ファイル取得

    Returns
    -------
    str
        $HOME/.pgpassの先頭行を取得して返す
    """
    if config_file is None:
        config_file = os.getenv('HOME') + '/.pgpass'
    with open(config_file, 'r') as fd:
        for line in fd:
            s_line = line.strip()
            if len(s_line) == 0 or s_line[0] == '#':
                continue
            host, port, dbname, user, password = s_line.split(':')
            return 'dbname=' + dbname + ' user=' + user + ' password=' + password + ' host=' + host + ' port=' + port


def select_and_write_location(ymd: str, result_file: BinaryIO) -> None:
    """
    Redshiftから該当日のデータを取得し、一時ファイルに書き出す

    Parameters
    ----------
    ymd:str
        該当日となる年月日 (YYYYMMDD)
    result_file: BinaryIO
        一時ファイルのio
    """
    connection_string = get_connection_string()
    with psycopg2.connect(connection_string) as conn, result_file as fd:
        with conn.cursor() as cursor:
            cursor.execute('select user_id, latitude, longitude, created_at from spectrum.location where created_date=\'' + ymd + '\'')
            for row in cursor:
                fd.write(bytes(','.join([str(x) for x in row]) + '\r\n', 'ascii'))


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
    try:
        logger.info('start.')

        with tempfile.TemporaryFile() as ftemp:
            # Redshiftからqueryし、一時ファイルに圧縮して書き出す
            with gzip.GzipFile(fileobj=ftemp, mode='w+b') as fout:
                select_and_write_location(ymd, fout)

            # 一時ファイルをS3にアップロードする
            ftemp.seek(0)
            s3.upload_fileobj(Fileobj=ftemp, Bucket=DOWNLOAD_BUCKET, Key=ymd + '.csv.zip')

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
