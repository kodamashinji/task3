# coding=utf-8

"""
加工処理用のEC2サーバを終了する
"""

import boto3
import logging
import os
from typing import Any, Dict

DEFAULT_EC2_INSTANCE_ID = 'i-0f2ace5d656e6fa62'

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def ec2_stop(instance_id: str) -> None:
    """
    EC2サーバを終了する

    Raises
        EC2終了に失敗したとき
    """
    logger.info('ec2_stop')
    client = boto3.client('ec2')
    response = client.stop_instances(
        InstanceIds=[instance_id]
    )
    logger.info(response)
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise Exception('ec2 stop error', response)


def lambda_handler(event: Any, context: Any) -> Dict[str, Any]:
    """
    EC2サーバを終了する

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
        instance_id = os.environ.get('EC2_INSTANCE_ID', DEFAULT_EC2_INSTANCE_ID)
        ec2_stop(instance_id)
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
