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
from collections.abc import Mapping

logger = logging.getLogger(__name__)

batch_client = None

def get_batch_client():
    global batch_client
    if batch_client is None:
        batch_client = boto3.client('batch')
    return batch_client


def submit_batch_job(payload, arn, queue='basic-ondemand', definition='geolambda-as-batch', name=None):
    # envvars
    STACK_PREFIX = getenv('CIRRUS_STACK')
    PAYLOAD_BUCKET = getenv('CIRRUS_PAYLOAD_BUCKET')

    if name is None:
        name = arn.split(':')[-1]

    # upload payload to s3
    url = f"s3://{PAYLOAD_BUCKET}/batch/{uuid.uuid1()}.json"
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
    response = get_batch_client().submit_job(**kwargs)
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

def property_match(feature, props):
    prop_checks = []
    for prop in props:
        prop_checks.append(feature['properties'].get(prop, '') == props[prop])
    return all(prop_checks)


# from https://gist.github.com/angstwad/bf22d1822c38a92ec0a9#gistcomment-2622319
def dict_merge(dct, merge_dct, add_keys=True):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    This version will return a copy of the dictionary and leave the original
    arguments untouched.
    The optional argument ``add_keys``, determines whether keys which are
    present in ``merge_dict`` but not ``dct`` should be included in the
    new dict.
    Args:
        dct (dict) onto which the merge is executed
        merge_dct (dict): dct merged into dct
        add_keys (bool): whether to add new keys
    Returns:
        dict: updated dict
    """
    dct = dct.copy()
    if not add_keys:
        merge_dct = {
            k: merge_dct[k]
            for k in set(dct).intersection(set(merge_dct))
        }

    for k, v in merge_dct.items():
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(merge_dct[k], Mapping)):
            dct[k] = dict_merge(dct[k], merge_dct[k], add_keys=add_keys)
        else:
            dct[k] = merge_dct[k]

    return dct


def recursive_compare(d1, d2, level='root', print=print):
    same = True
    if isinstance(d1, dict) and isinstance(d2, dict):
        if d1.keys() != d2.keys():
            same = False
            s1 = set(d1.keys())
            s2 = set(d2.keys())
            print(f'{level:<20} + {s1-s2} - {s2-s1}')
            common_keys = s1 & s2
        else:
            common_keys = set(d1.keys())

        for k in common_keys:
            same = same and recursive_compare(
                d1[k],
                d2[k],
                level=f'{level}.{k}',
            )

    elif isinstance(d1, list) and isinstance(d2, list):
        if len(d1) != len(d2):
            same = False
            print(f'{level:<20} len1={len(d1)}; len2={len(d2)}')
        common_len = min(len(d1), len(d2))

        for i in range(common_len):
            same = same and recursive_compare(
                d1[i],
                d2[i],
                level=f'{level}[{i}]',
            )

    elif d1 != d2:
        print(f'{level:<20} {d1} != {d2}')
        same = False

    else:
        # base case d1 == d2
        pass

    return same

