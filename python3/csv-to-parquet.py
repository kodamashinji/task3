# coding=utf-8
import boto3
import logging
import tempfile
import os
import datetime


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


def load_object(s3_object):
    # ex) s3_object is 'work/1566624557.csv'
    s3
    pass


def main():
    target_request_file, non_target_request_file = [None, None]
    try:
        logger.info('start.')
        object_list = list_request_objects()
        target_request_file, non_target_request_file = make_temporary_files()
        target_time = get_target_time()
        for s3_object in object_list:
            requests = load_object(s3_object)

        print(requests)
        print(target_request_file.name)
        print(non_target_request_file.name)
        print(target_time)
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
