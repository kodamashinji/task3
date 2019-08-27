# coding=utf-8
import boto3
import logging
import tempfile
import os
import datetime
import time
import smart_open
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_NAME = 'request-queue'
BUCKET = 'me32as8cme32as8c-task3-location'
FOLDER_WORK = 'work/'
FOLDER_PARTED = 'parted/'

s3 = boto3.client('s3')
tz = 9 * 60 * 60   # JST(+9:00)


#
# 作業用フォルダ(S3)のファイル一覧を取得する
#
def list_location_file():
    result = []

    # フォルダ以下のファイル一覧を取得
    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=FOLDER_WORK
    )

    # フォルダ自身以外をの一覧を追加
    result.extend([x['Key'] for x in response['Contents'] if x['Key'] != FOLDER_WORK])

    # 一度で取りきれなかった場合、取れなくなるまで繰り返す
    while 'NextContinuationToken' in response:
        token = response['NextContinuationToken']
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=FOLDER_WORK,
            ContinuationToken=token
        )
        result.extend([x['Key'] for x in response['Contents'] if x['Key'] != FOLDER_WORK])

    return result


#
# 基準となる時間 (今日の0:00)のunix_timeの取得
#
def get_base_time(unix_time):
    local_time = unix_time + tz
    local_base_time = local_time - local_time % (60 * 60 * 24)
    return local_base_time - tz


#
# レコードの基準時間毎に、保管用一時ファイルに振り分ける
#
def separate_location(file, temporary_file_dict):
    # ex) s3_object is 'work/1566624557.csv'
    with smart_open.open('s3://' + BUCKET + '/' + file, 'rb') as fd:
        for line in fd:
            timestamp, buffer = get_timestamp_and_buffer(line)
            if timestamp > 0:
                base_time = get_base_time(timestamp)
                if base_time not in temporary_file_dict:
                    temporary_file_dict[base_time] = tempfile.NamedTemporaryFile(delete=False)

                temp_file = temporary_file_dict[base_time]
                temp_file.write(buffer)

    return temporary_file_dict


#
# "user_id,latitude,longtude, timestamp"のtimestamp部分を取得し、行全体のbyte列を返す
# ただし、無効行の場合は[0, b'']を返す
#
def get_timestamp_and_buffer(line):
    s = line.decode('ascii').strip()  # binary -> str
    if len(s) > 0:
        try:
            timestamp = int(s.rsplit(',', 1)[1])
            buffer = bytes(s + '\n', 'ascii')
            return [timestamp, buffer]
        except IndexError:  # ","が無い
            pass
        except ValueError:  # timestampが数値じゃない
            pass

    return [0, b'']


#
# ファイルが存在し空では無いかどうか
#
def file_is_not_empty(f):
    return os.path.exists(f.name) and os.path.getsize(f.name) > 0


#
# 処理済みのファイルを削除
#
def remove_location_file(file_list):
    for file in file_list:
        s3.delete_object(Bucket=BUCKET, Key=file)


#
# Redshiftへのコネクション取得
#
def redshift_connect():
    pgpass = get_pgpass()
    host, port, database, user, password = pgpass.split(':')
    conn = psycopg2.connect(host=host, dbname=database, user=user, password=password, port=port)
    conn.autocommit = True
    return conn


#
# Redshiftへのコネクション設定ファイル取得
#
def get_pgpass():
    pgpass_file = os.getenv('HOME') + '/.pgpass'
    with open(pgpass_file, 'r') as fd:
        return fd.readline().strip()


#
# Redshiftにpartitionを追加
#
def add_partition_to_redshift(conn, ymd):
    with conn.cursor() as cur:
        cur.execute('alter table spectrum.location add if not exists partition(created_date=\'' + ymd + '\') '
                    + 'location \'s3://' + BUCKET + '/' + FOLDER_PARTED + 'created_date=' + ymd + '/\'')


def main():
    temporary_file_dict = dict()
    try:
        logger.info('start.')
        # ターゲットとなる全ファイル名を取得
        file_list = list_location_file()

        # データを基準時間毎に振り分けて一時ファイルに保管する
        for file in file_list:
            temporary_file_dict = separate_location(file, temporary_file_dict)

        # 一旦クローズ
        [t.close() for t in temporary_file_dict.values()]

        # 結果をS3に保管する
        now = int(time.time())
        today_base_time = get_base_time(now)
        upload_file_name = str(now) + '.csv'
        with redshift_connect() as conn:
            for base_time, temp_file in temporary_file_dict.items():
                if base_time == today_base_time:
                    s3.upload_file(Filename=temp_file.name, Bucket=BUCKET, Key=FOLDER_WORK + upload_file_name)
                else:
                    ymd = datetime.datetime.utcfromtimestamp(base_time + tz).strftime('%Y%m%d')  # YYYYMMDD
                    add_partition_to_redshift(conn, ymd)
                    s3.upload_file(Filename=temp_file.name, Bucket=BUCKET,
                                   Key=FOLDER_PARTED + 'created_date=' + ymd + '/' + upload_file_name)

        # 処理済みのファイルを削除
        remove_location_file(file_list)

        logger.info('finished.')
        return 'success'
    except Exception as e:
        logger.error(e)
    finally:
        [os.remove(t.name) for t in temporary_file_dict.values()]

    return 'error'


if __name__ == "__main__":
    main()
