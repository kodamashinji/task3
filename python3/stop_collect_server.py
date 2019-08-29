# coding=utf-8

"""
加工処理用のEC2サーバを終了する
"""

import boto3
import logging
from typing import Any

EC2_INSTANCE_ID = 'i-0f2ace5d656e6fa62'

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def ec2_stop() -> None:
    """
    EC2サーバを終了する

    Raises
        EC2終了に失敗したとき
    """
    logger.info('ec2_stop')
    client = boto3.client('ec2')
    response = client.stop_instances(
        InstanceIds=[
            EC2_INSTANCE_ID
        ]
    )
    logger.info(response)
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise Exception('ec2 stop error', response)


def lambda_handler(event: Any, context: Any):
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
    str
        "success" or "error"
    """
    try:
        logger.info('start.')
        ec2_stop()
        logger.info('finished.')
        return 'success'
    except Exception as e:
        logger.error(e)
        return 'error'
