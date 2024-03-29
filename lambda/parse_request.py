# coding=utf-8

"""
位置情報リクエストを受け取り、SQSにリクエストを積む
APIGateway経由でLambdaとして呼び出される

なお、全体の処理手順は以下の通り
[parse_request]  ->  store_request  ->  retrieve_request  -> collect_request
"""

import boto3
import logging
import os
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_QUEUE_NAME = 'request-queue'


def parse_request(json: Dict[str, Any]) -> str:
    """
    リクエストで来たJSONファイルをCSV文字列に変換する

    Parameters
    ----------
    json : Dict[str, Any]
        以下のようなJSONを表すdict
        (例)
        {
          "user_id": "cf5bff5c-2ffe-4f18-9593-bc666313f8c5"
          "location": {
            "lat_north_south": "N",
            "latitude": "35.744947",
            "lon_west_east": "E",
            "longitude": "139.720168"
          },
          "timestamp": 1555055157
        }

    Returns
    -------
    str
        "ユーザID,緯度,経度,タイムスタンプ"の文字列

    Raises
    ------
    ValueError
        不正なレコード
    """
    user_id = json['user_id']
    location = json['location']
    lat_mark = location['lat_north_south'].upper()
    latitude = float(location['latitude'])
    lng_mark = location['lon_west_east'].upper()
    longitude = float(location['longitude'])
    timestamp = int(json['timestamp'])
    if lat_mark == 'S':  # 南緯
        latitude = -latitude
    elif lat_mark != 'N':
        raise ValueError('Invalid latitude mark')
    if lng_mark == 'W':  # 西経
        longitude = -longitude
    elif lng_mark != 'E':
        raise ValueError('Invalid longitude mark')

    return user_id + ',' + str(latitude) + ',' + str(longitude) + ',' + str(timestamp)


def push_location(location: str, queue_name: str) -> None:
    """
    位置情報を表すCSVをSQSにpushする

    Parameters
    ----------
    location: str
        位置情報を表す文字列。parse_requestでSQSに積まれた文字列
        "ユーザID,緯度,経度,タイムスタンプ"の文字列
    queue_name: str
        キュー名。デフォルトはQUEUE_NAME
    """
    sqs = boto3.client('sqs')
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=location
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
        queue_name = os.environ.get('QUEUE_NAME', DEFAULT_QUEUE_NAME)
        location = parse_request(event)
        push_location(location, queue_name)
        logger.info('finished.')
        return {
            'statusCode': 200,
            'body': 'success',
        }
    except KeyError:
        logger.error('invalid json format')
    except ValueError:
        logger.error('invalid value')
    except Exception as e:
        logger.error(e)

    return {
        'statusCode': 500,
        'body': 'error',
    }


if __name__ == "__main__":
    lambda_handler(
        {
            'user_id': 'cf5bff5c-2ffe-4f18-9593-bc666313f8c5',
            'location': {
                'lat_north_south': 'N',
                'latitude': '35.744947',
                'lon_west_east': 'E',
                'longitude': '139.720168'
            },
            'timestamp': 1555055157
        },
        {}
    )
