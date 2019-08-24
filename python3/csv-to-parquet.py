# coding=utf-8
import boto3
import logging
import tempfile
import os
import datetime
import time
import smart_open

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_NAME = 'request-queue'
BUCKET = 'me32as8cme32as8c-task3-rawdata'
FOLDER_WORK = 'work/'
FOLDER_DONE = 'done/'

s3 = boto3.client('s3')
time_delta = datetime.timedelta(hours=9)


def list_request_objects():
    keys = []
    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=FOLDER_WORK
    )
    keys.extend([x['Key'] for x in response['Contents'] if x['Key'] != FOLDER_WORK])
    while 'NextContinuationToken' in response:
        token = response['NextContinuationToken']
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=FOLDER_WORK,
            ContinuationToken=token
        )
        keys.extend([x['Key'] for x in response['Contents'] if x['Key'] != FOLDER_WORK])

    return keys


def make_temporary_files():
    target_request_file = tempfile.NamedTemporaryFile(delete=False)
    non_target_request_file = tempfile.NamedTemporaryFile(delete=False)
    return [target_request_file, non_target_request_file]


def get_target_time():
    now = datetime.datetime.utcnow() + time_delta  # ローカル現在時刻
    utc_today = datetime.datetime(now.year, now.month, now.day) - time_delta  # ローカルの今日の0時のUTC
    target_time = int(
        datetime.datetime(
            utc_today.year,
            utc_today.month,
            utc_today.day,
            utc_today.hour,
            utc_today.minute,
            tzinfo=datetime.timezone.utc
        ).timestamp()
    )
    return target_time


def separate_request(key, fdt, fdn, target_time):
    # ex) s3_object is 'work/1566624557.csv'
    with smart_open.open('s3://' + BUCKET + '/' + key, 'rb') as fd:
        for line in fd:
            s = line.decode('ascii').strip()
            if len(s) > 0:
                t = int(s.rsplit(',', 1)[1])
                buffer = bytes(s + '\n', 'ascii')
                if t < target_time:
                    fdt.write(buffer)
                else:
                    fdn.write(buffer)


def store_request(target_file_name, non_target_file_name):
    filename = str(int(time.time())) + '.csv'

    if os.path.exists(target_file_name) and os.path.getsize(target_file_name) > 0:
        s3.upload_file(Filename=target_file_name, Bucket=BUCKET, Key=FOLDER_DONE + filename)

    if os.path.exists(non_target_file_name) and os.path.getsize(non_target_file_name) > 0:
        s3.upload_file(Filename=target_file_name, Bucket=BUCKET, Key=FOLDER_WORK + filename)


def remove_request_object(key_list):
    for key in key_list:
        # s3.delete_object(Bucket=BUCKET, Key=key)
        print(key)


def main():
    target_request_file, non_target_request_file = [None, None]
    try:
        logger.info('start.')
        # ターゲットとなり全ファイル名を取得
        key_list = list_request_objects()

        # 対象レコードと非対象レコード保管用の一時ファイルの作成(自動deleteされない)
        target_request_file, non_target_request_file = make_temporary_files()

        # 基準となる時間 (今日の0:00)のunixtimeの取得
        target_time = get_target_time()

        # ファイル毎に基準時間前後で、保管用一時ファイルに振り分ける
        with target_request_file as fdt, non_target_request_file as fdn:
            for key in key_list:
                separate_request(key, fdt, fdn, target_time)

        # 結果をS3に保管する
        store_request(target_request_file.name, non_target_request_file.name)

        # 処理済みのオブジェクトを削除
        remove_request_object(key_list)

        logger.info('finished.')
        return 'success'
    except Exception as e:
        logger.error(e)
    finally:
        if target_request_file is not None:
            os.remove(target_request_file.name)
        if non_target_request_file is not None:
            os.remove(non_target_request_file.name)

    return 'error'


if __name__ == "__main__":
    main()
