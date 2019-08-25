# coding=utf-8
import boto3
import logging
import tempfile
import os
import datetime
import time
import smart_open
import pandas
import pyarrow
import pyarrow.parquet as parquet

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_NAME = 'request-queue'
RAWDATA_BUCKET = 'me32as8cme32as8c-task3-rawdata'
STORED_BUCKET = 'me32as8cme32as8c-task3-stored'
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
        Bucket=RAWDATA_BUCKET,
        Prefix=FOLDER_WORK
    )

    # フォルダ自身以外をの一覧を追加
    result.extend([x['Key'] for x in response['Contents'] if x['Key'] != FOLDER_WORK])

    # 一度で取りきれなかった場合、取れなくなるまで繰り返す
    while 'NextContinuationToken' in response:
        token = response['NextContinuationToken']
        response = s3.list_objects_v2(
            Bucket=RAWDATA_BUCKET,
            Prefix=FOLDER_WORK,
            ContinuationToken=token
        )
        result.extend([x['Key'] for x in response['Contents'] if x['Key'] != FOLDER_WORK])

    return result


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
    with smart_open.open('s3://' + RAWDATA_BUCKET + '/' + file, 'rb') as fd:
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
def store_location(upload_file_name, target_file_name):
    s3.upload_file(Filename=upload_file_name, Bucket=RAWDATA_BUCKET, Key=target_file_name)


#
# 処理済みのファイルを削除
#
def remove_location_file(file_list):
    for file in file_list:
        s3.delete_object(Bucket=RAWDATA_BUCKET, Key=file)


#
# CSVからparquetファイルを作って、一時ファイルに書き出す
#
def create_parquet(csv_file, parquet_file):
    data_frame = pandas.read_csv(csv_file)
    table = pyarrow.Table.from_pandas(data_frame)
    parquet.write_table(table, parquet_file)


#
# parquetファイルをS3にアップロードする
def upload_parquet(parquet_file, target_file_name, base_time):
    target_key = 'TODO: target_key_name'
    s3.upload_file(Filename=parquet_file, Bucket=STORED_BUCKET, Key=target_key)


def main():
    retrieved_file, pushed_back_file, parquet_file = [None, None, None]
    try:
        logger.info('start.')
        # ターゲットとなる全ファイル名を取得
        file_list = list_location_file()

        # 対象レコードと非対象レコード保管用の一時ファイルの作成(自動deleteされない)
        retrieved_file = tempfile.NamedTemporaryFile(delete=False)
        pushed_back_file = tempfile.NamedTemporaryFile(delete=False)

        # 基準となる時間 (今日の0:00)のunixtimeの取得
        base_time = get_base_time()

        # ファイル毎に基準時間前後で、保管用一時ファイルに振り分ける
        with retrieved_file as fd_retrieved, pushed_back_file as fd_pushed_back:
            for file in file_list:
                separate_location(file, fd_retrieved, fd_pushed_back, base_time)

        # 結果をS3に保管する
        upload_filename = str(int(time.time()))
        if os.path.exists(retrieved_file.name) and os.path.getsize(retrieved_file.name) > 0:
            store_location(retrieved_file.name, FOLDER_DONE + upload_filename + '.csv')
            parquet_file = tempfile.NamedTemporaryFile(delete=False)
            create_parquet(retrieved_file.name, parquet_file.name)
            upload_parquet(parquet_file.name, upload_filename + '.parquet', base_time)

        if os.path.exists(pushed_back_file.name) and os.path.getsize(pushed_back_file.name) > 0:
            store_location(pushed_back_file.name, FOLDER_WORK + upload_filename + '.csv')

        # 処理済みのファイルを削除
        remove_location_file(file_list)

        logger.info('finished.')
        return 'success'
    except Exception as e:
        logger.error(e)
    finally:
        for file in [f for f in [retrieved_file, pushed_back_file, parquet_file] if f is not None]:
            os.remove(file.name)

    return 'error'


if __name__ == "__main__":
    main()
