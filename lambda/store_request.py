# coding=utf-8

"""
SQSから取り出したリクエストデータ("ユーザID,緯度,経度,タイムスタンプ")を、S3作業用フォルダ(s://.../work)に保存する
CloudWatchによって定期的にlambdaとして呼び出される

なお、全体の処理手順は以下の通り
parse_request  ->  [store_request]  ->  retrieve_request  -> collect_request
"""

import boto3
import logging
import time
from typing import Any, List, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_NAME = 'request-queue'
BUCKET = 'me32as8cme32as8c-task3-location'
FOLDER = 'work/'


def retrieve_location(queue_name: str = QUEUE_NAME) -> List[str]:
    """
    SQSに蓄えられたリクエストを取得し、重複を取り除いた位置情報一覧を取得する

    Parameters
    ----------
    queue_name: str
        リクエストを取得するQUEUEの名前

    Returns
    ------
    List[str]
        "ユーザID,緯度,経度,タイムスタンプ"の文字列のリスト
    """
    sqs = boto3.client('sqs')
    retrieved = set()  # 重複チェック用
    result = list()    # メソッドの結果
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    while True:
        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        if 'Messages' not in messages or len(messages['Messages']) == 0:  # キューが空か
            break

        for message in messages['Messages']:
            handle, md5, body = [message[x] for x in ['ReceiptHandle', 'MD5OfBody', 'Body']]
            if md5 not in retrieved:  # メッセージが重複していなければ
                retrieved.update({md5})
                result.append(body)
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=handle)

    return result


def write_location(file_name: str, locations: List[str], bucket: str = BUCKET, prefix: str = FOLDER) -> None:
    """
    位置情報を作業用フォルダ(S3)に書き込む
    Parameters
    ----------
    file_name: str
        書き出すオブジェクトの名前

    locations: List[str]
        retrieve_location()で取得された結果。"ユーザID,緯度,経度,タイムスタンプ"の文字列のリスト

    bucket: str
        オブジェクトを書き込むS3バケット

    prefix: str
        オブジェクトのprefix
    """
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket,
        Key=prefix + file_name,
        Body="\n".join(locations) + "\n"
    )


def lambda_handler(event: Any, context: Any) -> Dict[str, Any]:
    """
    Lambdaからinvokeされる関数

    Parameters
    ----------
    event: Any
        未使用
    context: Any
        未使用

    Returns
    ------
    Dict[str, Any]
        "success" or "error"
    """
    try:
        logger.info('start.')
        locations = retrieve_location()
        if len(locations) > 0:
            file_name = str(int(time.time())) + '.csv'
            write_location(file_name, locations)
        logger.info('finished.')
        return {
            'statusCode': 200,
            'body': 'success',
        }
    except Exception as e:
        logger.error(e)

    return {
        'statusCode': 500,
        'body': 'error',
    }


if __name__ == "__main__":
    lambda_handler({}, {})
