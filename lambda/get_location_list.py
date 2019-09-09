# coding=utf-8

"""
S3に配置された日毎の位置情報ファイルを取得するため、APIGatewayから呼び出されるlambda
"""

import boto3
import logging
import os
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_DOWNLOAD_BUCKET = 'me32as8cme32as8c-task3-download'

s3 = boto3.client('s3')


def get_key(event: Dict[str, Any]) -> str:
    """
    event引数から取得したオブジェクトのS3上のキーを取得する

    Parameters
    ----------
    event: Dict[str, Any]
        lambda_handerに渡されたevent
        {
            "ymd": "YYYYMMDD"
        }
        の形式

    Returns
    -------
    str:
        対象となるオブジェクトのS3上のキー

    Raises
    ------
    KeyError
        eventにymdキーが含まれない
    """
    return event['ymd'] + '.csv.gz'


def check_file(key: str, bucket: str) -> None:
    """
    S3上に指定されたキーのオブジェクトが存在するかどうかを調べる

    Parameters
    ----------
    key: str
        対象となるS3オブジェクトのキー
    bucket: str
        キーが含まれるS3バケット名

    Raises
    ------
    botocore.exceptions.ClientError
        オブジェクトが無ければ例外
    botocore.exceptions.ParamValidationError
        オブジェクトが無ければ例外
    """
    s3.head_object(Bucket=bucket, Key=key)


def get_presigned_url(key: str, bucket: str) -> str:
    """
    指定されたキーの署名付きURLを返す

    Parameters
    ----------
    key: str
        対象となるS3オブジェクトのキー
    bucket: str
        キーが含まれるS3バケット名

    Returns
    -------
    str
        S3オブジェクトに対する署名付きURL
    """
    return s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': bucket, 'Key': key},
        ExpiresIn=300,
        HttpMethod='GET'
    )


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Lambdaからinvokeされる関数

    Parameters
    ----------
    event: Dict[str, Any]
        APIGateway経由で与えられたJSONデータをdictにしたもの
    context: Any
        未使用

    Returns
    ------
    Dict[str, Any]
        "success" or "error"
    """
    try:
        logger.info('start.')
        bucket = os.environ.get('DOWNLOAD_BUCKET', DEFAULT_DOWNLOAD_BUCKET)

        key = get_key(event)
        check_file(key, bucket)
        location = get_presigned_url(key, bucket)
        logger.info('finished.')
        return {
            'Location': location
        }
    except Exception as e:
        logger.error(e)
        raise Exception('not found')
