# coding=utf-8
import boto3
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_NAME = 'request-queue'
BUCKET = 'me32as8cme32as8c-task3-rawdata'
FOLDER = 'work/'


def retrieve_location():
    sqs = boto3.client('sqs')
    retrieved = set()
    result = list()
    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)['QueueUrl']
    while True:
        messages = sqs.receive_message(QueueUrl=queue_url)
        if 'Messages' not in messages or len(messages['Messages']) == 0:  # キューが空か
            break
        message = messages['Messages'][0]
        handle, md5, body = [message[x] for x in ['ReceiptHandle', 'MD5OfBody', 'Body']]
        if md5 not in retrieved:
            retrieved.update({md5})
            result.append(body)
            # sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=handle)

    return result


def write_location(locations):
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=BUCKET,
        Key=FOLDER + str(int(time.time())) + ".csv",
        Body="\n".join(locations)
    )


def lambda_handler(event, context):
    try:
        logger.info('start.')
        locations = retrieve_location()
        if len(locations) > 0:
            write_location(locations)
        logger.info('finished.')
        return 'success'
    except Exception as e:
        logger.error(e)

    return 'error'


if __name__ == "__main__":
    lambda_handler({}, {})
