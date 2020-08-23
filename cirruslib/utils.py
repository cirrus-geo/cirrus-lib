import boto3
import json
import logging
import requests
import uuid

from boto3utils import s3
from dateutil.parser import parse as dateparse
from os import getenv
from string import Formatter, Template
from typing import Dict, Optional, List

# configure logger - CRITICAL, ERROR, WARNING, INFO, DEBUG
logger = logging.getLogger(__name__)
logger.setLevel(getenv('CIRRUS_LOG_LEVEL', 'INFO'))

batch_client = boto3.client('batch')


def submit_batch_job(payload, arn, queue='basic-ondemand', definition='geolambda-as-batch', name=None):
    # envvars
    STACK_PREFIX = getenv('CIRRUS_STACK')
    CATALOG_BUCKET = getenv('CIRRUS_CATALOG_BUCKET')

    if name is None:
        name = arn.split(':')[-1]

    # upload payload to s3
    url = f"s3://{CATALOG_BUCKET}/batch/{uuid.uuid1()}.json"
    s3().upload_json(payload, url)
    kwargs = {
        'jobName': name,
        'jobQueue': f"{STACK_PREFIX}-{queue}",
        'jobDefinition': f"{STACK_PREFIX}-{definition}",
        'parameters': {
            'lambda_function': arn,
            'url': url
        },
        'containerOverrides': {
            'vcpus': 1,
            'memory': 512,
        }
    }
    logger.debug(f"Submitted batch job with payload {url}")
    response = batch_client.submit_job(**kwargs)
    logger.debug(f"Batch response: {response}")


def get_path(item: Dict, template: str='${collection}/${id}') -> str:
    """Get path name based on STAC Item and template string

    Args:
        item (Dict): A STAC Item.
        template (str, optional): Path template using variables referencing Item fields. Defaults to '${collection}/${id}'.

    Returns:
        [str]: A path name
    """
    _template = template.replace(':', '__colon__')
    subs = {}
    for key in [i[1] for i in Formatter().parse(_template.rstrip('/')) if i[1] is not None]:
        # collection
        if key == 'collection':
            subs[key] = item['collection']
        # ID
        elif key == 'id':
            subs[key] = item['id']
        # derived from date
        elif key in ['year', 'month', 'day']:
            dt = dateparse(item['properties']['datetime'])
            vals = {'year': dt.year, 'month': dt.month, 'day': dt.day}
            subs[key] = vals[key]
        # Item property
        else:
            subs[key] = item['properties'][key.replace('__colon__', ':')]
    return Template(_template).substitute(**subs).replace('__colon__', ':')