# coding=utf-8
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_NAME = 'request-queue'


#
# リクエストで来たJSONファイルをCSV文字列に変換する
#
def parse_request(json):
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


#
# 位置情報を表すCSVをSQSにpushする
# CSV形式は、"ユーザID,緯度(南緯は負),経度(西経は負),タイムスタンプ(unixtime)"
#
def push_location(location):
    sqs = boto3.client('sqs')
    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)['QueueUrl']
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=location
    )


def lambda_handler(event, context):
    try:
        logger.info('start.')
        location = parse_request(event)
        push_location(location)
        logger.info('finished.')
        return 'success'
    except KeyError as e:
        logger.error('invalid json format')
    except ValueError as e:
        logger.error('invalid value')
    except Exception as e:
        logger.error(e)

    return 'error'


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
