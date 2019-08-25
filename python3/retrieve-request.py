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
time_delta = datetime.timedelta(hours=9)  # 作業場所の時差


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
# 対象の位置情報と非対象の位置情報を書き込むための一時ファイルを作成
#
def make_temporary_file():
    return [tempfile.NamedTemporaryFile(delete=False), tempfile.NamedTemporaryFile(delete=False)]


#
# 基準となる時間 (今日の0:00)のunixtimeの取得
#
def get_base_time():
    now = datetime.datetime.utcnow() + time_delta  # ローカル現在時刻
    utc_today = datetime.datetime(now.year, now.month, now.day) - time_delta  # ローカルの今日の0時のUTC
    return int(
        datetime.datetime(
            utc_today.year,
            utc_today.month,
            utc_today.day,
            hour=utc_today.hour,
            minute=utc_today.minute,
            tzinfo=datetime.timezone.utc
        ).timestamp()
    )


#
# 基準時間前後で、保管用一時ファイルに振り分ける
#
def separate_location(file, fd_retrieved, fd_pushed_back, base_time):
    # ex) s3_object is 'work/1566624557.csv'
    with smart_open.open('s3://' + BUCKET + '/' + file, 'rb') as fd:
        for line in fd:
            timestamp, buffer = get_timestamp_and_buffer(line)
            if timestamp > 0:
                if timestamp < base_time:
                    fd_retrieved.write(buffer)
                else:
                    fd_pushed_back.write(buffer)


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
# 一時保管ファイルをS3にアップロードする
#
def store_location(retrieved_file_name, pushed_back_file_name):
    # 保存するファイル名
    filename = str(int(time.time())) + '.csv'

    if os.path.exists(retrieved_file_name) and os.path.getsize(retrieved_file_name) > 0:
        s3.upload_file(Filename=retrieved_file_name, Bucket=BUCKET, Key=FOLDER_DONE + filename)

    if os.path.exists(pushed_back_file_name) and os.path.getsize(pushed_back_file_name) > 0:
        s3.upload_file(Filename=pushed_back_file_name, Bucket=BUCKET, Key=FOLDER_WORK + filename)


#
# 処理済みのファイルを削除
#
def remove_location_file(file_list):
    for file in file_list:
        s3.delete_object(Bucket=BUCKET, Key=file)


def main():
    retrieved_file, pushed_back_file = [None, None]
    try:
        logger.info('start.')
        # ターゲットとなる全ファイル名を取得
        file_list = list_location_file()

        # 対象レコードと非対象レコード保管用の一時ファイルの作成(自動deleteされない)
        retrieved_file, pushed_back_file = make_temporary_file()

        # 基準となる時間 (今日の0:00)のunixtimeの取得
        base_time = get_base_time()

        # ファイル毎に基準時間前後で、保管用一時ファイルに振り分ける
        with retrieved_file as fd_retrieved, pushed_back_file as fd_pushed_back:
            for file in file_list:
                separate_location(file, fd_retrieved, fd_pushed_back, base_time)

        # 結果をS3に保管する
        store_location(retrieved_file.name, pushed_back_file.name)

        # 処理済みのファイルを削除
        remove_location_file(file_list)

        logger.info('finished.')
        return 'success'
    except Exception as e:
        logger.error(e)
    finally:
        if retrieved_file is not None:
            os.remove(retrieved_file.name)
        if pushed_back_file is not None:
            os.remove(pushed_back_file.name)

    return 'error'


if __name__ == "__main__":
    main()
