import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_NAME = 'request-queue'


def dequeue_from_sqs():
    sqs = boto3.client('sqs')
    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)['QueueUrl']
    digest_set = set()     # キュー内の重複メッセージを弾くための確認用SET
    request_list = list()  # 関数のリザルト
    while True:
        result = sqs.receive_message(QueueUrl=queue_url)
        if 'Messages' not in result or len(result['Messages']) == 0:
            break
        message = result['Messages'][0]
        md5 = message['MD5OfBody']
        handle = message['ReceiptHandle']
        body = message['Body']
        if md5 not in digest_set:
            digest_set.update({md5})
            request_list.append(body)
        #sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=handle)

    return request_list


def copy_to_s3(requests):
    s3 = boto3.client('s3')
    pass


def lambda_handler(event, context):
    try:
        logger.info('start.')
        requests = dequeue_from_sqs()
        if len(requests) > 0:
            copy_to_s3(requests)
        print(requests)
        logger.info('finished.')
        return 'success'
    except Exception as e:
        print("ERROR")
        logger.error(e)

    return 'error'


if __name__ == "__main__":
    lambda_handler({}, '')
