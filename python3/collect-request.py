# coding=utf-8
import boto3
import logging
import psycopg2
import time
import datetime
import os
import tempfile
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_NAME = 'request-queue'
DOWNLOAD_BUCKET = 'me32as8cme32as8c-task3-download'

s3 = boto3.client('s3')
tz = 9 * 60 * 60   # JST(+9:00)


#
# Redshiftへのコネクション取得
#
def redshift_connect():
    pgpass = get_pgpass()
    host, port, database, user, password = pgpass.split(':')
    conn = psycopg2.connect(host=host, dbname=database, user=user, password=password, port=port)
    return conn


#
# Redshiftへのコネクション設定ファイル取得
#
def get_pgpass():
    pgpass_file = os.getenv('HOME') + '/.pgpass'
    with open(pgpass_file, 'r') as fd:
        return fd.readline().strip()


#
# Redshiftから該当日のデータを取得し、一時ファイルに書き出す
#
def select_and_write_location(ymd, result_file):
    with redshift_connect() as conn, result_file as fd:
        with conn.cursor() as cursor:
            cursor.execute('select user_id, latitude, longitude, created_at from spectrum.location where created_date=\'' + ymd + '\'')
            for row in cursor:
                fd.write(bytes(','.join([str(x) for x in row]) + '\r\n', 'ascii'))


def main(ymd):
    result_file = None
    try:
        logger.info('start.')

        # 結果を保存する一時ファイル
        result_file = tempfile.NamedTemporaryFile(delete=False)

        # Redshiftからqueryし、一時ファイルに書き出す
        select_and_write_location(ymd, result_file)

        # 一時ファイルをS3にアップロードする
        s3.upload_file(Filename=result_file.name, Bucket=DOWNLOAD_BUCKET, Key=ymd + '.csv')

        logger.info('finished.')
        return 'success'
    except Exception as e:
        logger.error(e)
    finally:
        if result_file is not None:
            os.remove(result_file.name)

    return 'error'


if __name__ == "__main__":
    if len(sys.argv) == 1:  # 引数なしの場合は昨日のデータ
        local_time = time.time() + tz
        local_yesterday_base_time = local_time - (local_time % (60 * 60 * 24)) - (60 * 60 * 24)
        ymd = datetime.datetime.utcfromtimestamp(local_yesterday_base_time).strftime('%Y%m%d')  # YYYYMMDD
    else:
        ymd = sys.argv[1]
    main(ymd)

