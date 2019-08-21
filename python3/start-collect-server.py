import boto3
import logging

EC2_INSTANCE_ID = 'i-0f2ace5d656e6fa62'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def ec2_start():
    logger.info('ec2_start')
    client = boto3.client('ec2')
    response = client.start_instances(
        InstanceIds=[
            EC2_INSTANCE_ID
        ]
    )
    logger.info(response)
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise Exception('ec2 start error', response)

def lambda_handler(event, context):
    try:
        logger.info('start.')
        boto3.setup_default_session(region_name='ap-northeast-1')
        ec2_start()
        logger.info('finished.')
        return 'success'
    except Exception as e:
        logger.error(e)
        return 'error'
