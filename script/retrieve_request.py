# coding=utf-8

"""
S3作業用フォルダ(s3://..../work)にあるオブジェクトを読み込み、日付毎に振り分ける。
振り分けた結果は、S3格納用フォルダ(s3://..../parted)に書き出される。
ただし、このプログラムを実行した当日のデータは一時保管用フォルダに戻されて、翌日以降に書き出される。

なお、全体の処理手順は以下の通り
parse_request  ->  store_request  ->  [retrieve_request]  -> collect_request
"""

import boto3
import logging
import tempfile
import os
import datetime
import time
import smart_open
import gzip
import shutil
import psycopg2
import psycopg2.extensions
from typing import Dict, List, BinaryIO, Type
from get_connection_string import get_connection_string

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_BUCKET_LOCATION = 'me32as8cme32as8c-task3-location'
DEFAULT_FOLDER_WORK = 'work/'
DEFAULT_FOLDER_PARTED = 'parted/'

s3 = boto3.client('s3')
tz = 9 * 60 * 60   # JST(+9:00)


def list_location_file(bucket: str, prefix: str) -> List[str]:
    """
    作業用フォルダ(S3)のファイル一覧を取得する

    Parameters
    ----------
    bucket: str
        オブジェクトを書き込むS3バケット
    prefix: str
        オブジェクトのprefix

    Returns
    ------
    List[str]
        ファイル名の文字列一覧。
        (例) "work/123456.csv"
    """
    result = []

    # フォルダ以下のファイル一覧を取得
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    # フォルダ自身以外を一覧を追加
    result.extend([x['Key'] for x in response['Contents'] if x['Key'] != prefix])

    # 一度で取りきれなかった場合、取れなくなるまで繰り返す
    while 'NextContinuationToken' in response:
        token = response['NextContinuationToken']
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=token)
        result.extend([x['Key'] for x in response['Contents'] if x['Key'] != prefix])

    return result


def get_base_time(unix_time: int) -> int:
    """
    指定された時間を含む日付の0:00のunix時間を取得する (日本時間)

    Parameters
    ----------
    unix_time: int
        日付を計算するための基準時間

    Returns
    -------
    int
        unix_timeを含む日付の0:00のunix時間
    """
    local_time = unix_time + tz
    local_base_time = local_time - local_time % (60 * 60 * 24)

    return local_base_time - tz


def separate_location(file: str, temporary_file_dict: Dict[int, BinaryIO], bucket: str) -> dict:
    """
    レコードの基準時間毎に、保管用一時ファイルに振り分ける

    Parameters
    ----------
    file: str
        ファイル名 (例: 'work/1566624557.csv')
    temporary_file_dict: Dict[int, BinaryIO]
        作成されたtempfile.NamedTemporaryFileを保管するdict
    bucket: str
        オブジェクトを書き込むS3バケット

    Returns
    -------
    Dict[int, BinaryIO]
        作成されたtempfile.NamedTemporaryFileを保管するdict
    """
    with smart_open.open('s3://' + bucket + '/' + file, 'rb') as fd:
        for line in fd:
            timestamp, buffer = get_timestamp_and_buffer(line)
            if timestamp > 0:                                   # 正常行か?
                base_time = get_base_time(timestamp)
                if base_time not in temporary_file_dict:
                    temporary_file_dict[base_time] = tempfile.NamedTemporaryFile(delete=False)

                temp_file = temporary_file_dict[base_time]
                temp_file.write(buffer)

    return temporary_file_dict


def get_timestamp_and_buffer(line: bytes) -> (int, bytes):
    """
    "user_id,latitude,longitude,timestamp"のtimestamp部分を取得し、行全体のbyte列を返す
    ただし、無効行の場合は[0, b'']を返す

    Parameters
    ----------
    line: byte
        レコード1行のbyte列

    Returns
    -------
    int, bytes
        タイムスタンプ(レコードの最後のフィールド), レコード行 (常に改行付き)
    """
    s = line.decode('ascii').strip()               # bytes -> str
    if len(s) > 0:
        try:
            fields = s.split(',')
            if len(fields) == 4:
                timestamp = int(fields[3]) # 最後のフィールド
                buffer = bytes(s + '\n', 'ascii')      # str -> bytes
                return timestamp, buffer
        except IndexError:  # ","が無い
            pass
        except ValueError:  # timestampが数値じゃない
            pass

    return 0, b''


def remove_location_file(file_list: List[str], bucket: str) -> None:
    """
    処理済みのファイルを削除

    Parameters
    ----------
    file_list: List[str]
        ファイル名の文字列一覧。
        (例) "work/123456.csv"
    bucket: str
        オブジェクトを書き込むS3バケット
    """
    for file in file_list:
        s3.delete_object(Bucket=bucket, Key=file)


def add_partition_to_redshift(conn: Type[psycopg2.extensions.connection], ymd: str, bucket: str, prefix: str) -> None:
    """
    Redshiftにpartitionを追加

    Parameters
    ----------
    conn: Type[psycopg2.extensions.connection]
        Redshiftへの接続オブジェクト
        redshift_connect()で取得
    ymd: str
        年月日(YYYYMMDD)の文字列
    bucket: str
        オブジェクトを書き込むS3バケット
    prefix: str
        バケットのprefix
    """
    with conn.cursor() as cur:
        cur.execute('alter table spectrum.location add if not exists partition(created_date=\'' + ymd + '\') '
                    + 'location \'s3://' + bucket + '/' + prefix + 'created_date=' + ymd + '/\'')


def get_date_str(unix_time: int) -> str:
    """
    指定されたunix_timeをJSTの日付に変換する

    Parameters
    ----------
    unix_time: int
        日付を求める時間

    Returns
    -------
    str
        "YYYYMMDD"形式の文字列(JST)
    """
    return datetime.datetime.utcfromtimestamp(unix_time + tz).strftime('%Y%m%d')  # YYYYMMDD


def compress_and_upload(source_file_name: str, upload_file_name: str, ymd: str, bucket: str, prefix: str) -> None:
    """
    指定されたファイルを圧縮し、S3にアップロードする

    Parameters
    ----------
    source_file_name: str
        コピー元となるローカルソース
    upload_file_name: str
        アップロードするオブジェクト名。実際のオブジェクト名は、このオブジェクト名に".gz"が付与される
    ymd: str
        アップロードするオブジェクトが格納されるパーティションフォルダ用の日付
    bucket: str
        オブジェクトを格納するバケット名
    prefix: str
        オブジェクトを格納するフォルダ(prefix)
    """
    with tempfile.TemporaryFile() as fd_temp:
        # 圧縮
        with open(source_file_name, 'rb') as fd_in, gzip.GzipFile(fileobj=fd_temp, mode='w+b') as fd_out:
            shutil.copyfileobj(fd_in, fd_out)

        # アップロード
        fd_temp.seek(0)
        s3.upload_fileobj(Fileobj=fd_temp, Bucket=bucket,
                          Key=prefix + 'created_date=' + ymd + '/' + upload_file_name + '.gz')


def main() -> str:
    """
    メイン

    Returns
    -------
    str
        "success" or "error"
    """
    bucket = os.environ.get('BUCKET_LOCATION', DEFAULT_BUCKET_LOCATION)
    folder_work = os.environ.get('FOLDER_WORK', DEFAULT_FOLDER_WORK)
    folder_parted = os.environ.get('FOLDER_PARTED', DEFAULT_FOLDER_PARTED)

    temporary_file_dict = dict()
    try:
        logger.info('start.')
        # ターゲットとなる全ファイル名を取得
        file_list = list_location_file(bucket, folder_work)

        # データを基準時間毎に振り分けて一時ファイルに保管する
        for file in file_list:
            temporary_file_dict = separate_location(file, temporary_file_dict, bucket)

        # 一旦クローズ
        [t.close() for t in temporary_file_dict.values()]

        # 結果をS3に保管する
        now = int(time.time())
        today_base_time = get_base_time(now)
        upload_file_name = str(now) + '.csv'
        connection_string = get_connection_string()
        with psycopg2.connect(connection_string) as conn:
            conn.autocommit = True  # ALTER TABLEはBEGIN内で使えないので、autocommit=Trueにしておく

            for base_time, temp_file in temporary_file_dict.items():
                if base_time == today_base_time:
                    # 本日分のデータはwork/ディレクトリに送り返す
                    s3.upload_file(Filename=temp_file.name, Bucket=bucket, Key=folder_work + upload_file_name)
                else:
                    # 前日までのデータは圧縮してS3のparted/フォルダにコピーする
                    ymd = get_date_str(base_time)  # YYYYMMDD
                    add_partition_to_redshift(conn, ymd, bucket, folder_parted)
                    compress_and_upload(temp_file.name, upload_file_name, ymd, bucket, folder_parted)

        # 処理済みのファイルを削除
        remove_location_file(file_list, bucket)

        logger.info('finished.')
        return 'success'
    except Exception as e:
        logger.error(e)
    finally:
        [os.remove(t.name) for t in temporary_file_dict.values()]

    return 'error'


if __name__ == "__main__":
    main()
