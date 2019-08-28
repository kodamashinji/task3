# coding=utf-8

"""
S3作業用フォルダ(s3://..../work)にあるオブジェクトを読み込み、日付毎に振り分ける。
振り分けた結果は、S3格納用フォルダ(s3://..../parted)に書き出される。
ただし、このプログラムを実行した当日のデータは一時保管用フォルダに戻されて、翌日以降に書き出される。

なお、全体の処理手順は以下の通り
parse-request  ->  store-request  ->  [retrieve-request]  -> collect-request
"""

import boto3
import logging
import tempfile
import os
import datetime
import time
import smart_open
import psycopg2
import psycopg2.extensions
from typing import Dict, List, BinaryIO, Type

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_NAME = 'request-queue'
BUCKET = 'me32as8cme32as8c-task3-location'
FOLDER_WORK = 'work/'
FOLDER_PARTED = 'parted/'

s3 = boto3.client('s3')
tz = 9 * 60 * 60   # JST(+9:00)


def list_location_file() -> List[str]:
    """
    作業用フォルダ(S3)のファイル一覧を取得する

    Returns
    ------
    List[str]
        ファイル名の文字列一覧。
        (例) "work/123456.csv"
    """
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


def separate_location(file: str, temporary_file_dict: Dict[int, BinaryIO]) -> dict:
    """
    レコードの基準時間毎に、保管用一時ファイルに振り分ける

    Parameters
    ----------
    file: str
        ファイル名 (例: 'work/1566624557.csv')
    temporary_file_dict: Dict[int, BinaryIO]
        作成されたtempfile.NamedTemporaryFileを保管するdict

    Returns
    -------
    Dict[int, BinaryIO]
        作成されたtempfile.NamedTemporaryFileを保管するdict
    """
    with smart_open.open('s3://' + BUCKET + '/' + file, 'rb') as fd:
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
    "user_id,latitude,longtude, timestamp"のtimestamp部分を取得し、行全体のbyte列を返す
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
            timestamp = int(s.rsplit(',', 1)[1])   # 最後のフィールド
            buffer = bytes(s + '\n', 'ascii')      # str -> bytes
            return timestamp, buffer
        except IndexError:  # ","が無い
            pass
        except ValueError:  # timestampが数値じゃない
            pass

    return 0, b''


def remove_location_file(file_list: List[str]) -> None:
    """
    処理済みのファイルを削除

    Parameters
    ----------
    file_list: List[str]
        ファイル名の文字列一覧。
        (例) "work/123456.csv"
    """
    for file in file_list:
        s3.delete_object(Bucket=BUCKET, Key=file)


def redshift_connect() -> Type[psycopg2.extensions.connection]:
    """
    Redshiftへのコネクション取得
    接続情報は$HOME/.pgpassから取得する

    Returns
    -------
    Type[psgcopg2.extensions.connection]
        psycopg2.connectによるconnectionオブジェクト
    """
    pgpass = get_pgpass()
    host, port, database, user, password = pgpass.split(':')
    conn = psycopg2.connect(host=host, dbname=database, user=user, password=password, port=port)
    conn.autocommit = True
    return conn


def get_pgpass() -> str:
    """
    Redshiftへのコネクション設定ファイル取得

    Returns
    -------
    str
        $HOME/.pgpassの先頭行を取得して返す
    """
    pgpass_file = os.getenv('HOME') + '/.pgpass'
    with open(pgpass_file, 'r') as fd:
        return fd.readline().strip()


def add_partition_to_redshift(conn: Type[psycopg2.extensions.connection], ymd: str) -> None:
    """
    Redshiftにpartitionを追加

    Parameters
    ----------
    conn: Type[psycopg2.extensions.connection]
        Redshiftへの接続オブジェクト
        redshift_connect()で取得
    ymd: str
        年月日(YYYYMMDD)の文字列
    """
    with conn.cursor() as cur:
        cur.execute('alter table spectrum.location add if not exists partition(created_date=\'' + ymd + '\') '
                    + 'location \'s3://' + BUCKET + '/' + FOLDER_PARTED + 'created_date=' + ymd + '/\'')


def main() -> str:
    """
    メイン

    Returns
    -------
    str
        "success" or "error"
    """
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
