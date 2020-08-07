from __future__ import annotations

import boto3
import json
import logging
import os
import re
import uuid

from boto3utils import s3
from cirruslib.statedb import StateDB
from cirruslib.transfer import get_s3_session
from cirruslib.utils import get_path
from typing import Dict, Optional, List

# envvars
LOG_LEVEL = os.getenv('CIRRUS_LOG_LEVEL', 'INFO')
DATA_BUCKET = os.getenv('CIRRUS_DATA_BUCKET', None)
CATALOG_BUCKET = os.getenv('CIRRUS_CATALOG_BUCKET', None)
PROCESS_QUEUE = os.getenv('CIRRUS_PROCESS_QUEUE', None)
PUBLISH_TOPIC_ARN = os.getenv('CIRRUS_PUBLISH_TOPIC_ARN', None)

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

# clients
statedb = StateDB()
sqsclient = boto3.client('sqs')
snsclient = boto3.client('sns')
sqs_url = None
if PROCESS_QUEUE:
    sqs_url = sqsclient.get_queue_url(QueueName=PROCESS_QUEUE)['QueueUrl']


class Catalog(dict):

    def __init__(self, *args, state_item=None, **kwargs):
        """Initialize a Catalog, verify required fields, and assign an ID

        Args:
            state_item (Dict, optional): Dictionary of entry in StateDB. Defaults to None.
        """
        super(Catalog, self).__init__(*args, **kwargs)

        # validate process block
        assert(self['type'] == 'FeatureCollection')
        assert('process' in self)
        assert('output_options' in self['process'])
        assert('collections' in self)
        assert('workflow' in self['process'])

        # convert old functions field to tasks
        if 'functions' in self['process']:
            self['process']['tasks'] = self['process'].pop('functions')

        assert('tasks' in self['process'])

        # TODO - validate with a JSON schema
        #if schema:
        #    pass
        # For now, just make check that there is at least one item
        assert(len(self['features']) > 0)
        for item in self['features']:
            if 'links' not in item:
                item['links'] = []

        # Input collections
        if 'input_collections' not in self['process']:
            cols = sorted(list(set([i['collection'] for i in self['features'] if 'collection' in i])))
            self['process']['input_collections'] = cols if len(cols) != 0 else 'none'

        # generate ID
        collections_str = '/'.join(self['process']['input_collections'])
        items_str = '/'.join(sorted(list([i['id'] for i in self['features']])))
        self['id'] = f"{collections_str}/workflow-{self['process']['workflow']}/{items_str}"

        self.state_item = state_item

    # assign collections to Items given a mapping of Col ID: ID regex
    def assign_collections(self):
        """Assign new collections to all Items (features) in Catalog
            based on self['process']['output_options']['collections']
        """
        collections = self['process']['output_options']['collections']
        for item in self['features']:
            for col in collections:
                regex = re.compile(collections[col])
                if regex.match(item['id']):
                    logger.debug(f"Setting {item['id']} collection to {col}")
                    item['collection'] = col

    def get_payload(self) -> Dict:
        """Get original payload for this Catalog

        Returns:
            Dict: Cirrus Input Catalog
        """
        payload = json.dumps(self)
        if CATALOG_BUCKET and len(payload.encode('utf-8')) > 30000:
            assert(CATALOG_BUCKET)
            url = f"s3://{CATALOG_BUCKET}/payloads/{uuid.uuid1()}.json"
            s3().upload_json(self, url)
            return {'url': url}
        else:
            return dict(self)

    # publish the items in this catalog
    def publish_to_s3(self, bucket, public=False) -> List:
        """Publish all Items to s3

        Args:
            bucket (str): Name of bucket to publish to
            public (bool, optional): Make published STAC Item public. Defaults to False.

        Returns:
            List: List of s3 URLs to published Items
        """
        opts = self['process'].get('output_options', {})
        s3urls = []
        for item in self['features']:
            # determine URL of data bucket to publish to- always do this
            url = os.path.join(get_path(item, opts.get('path_template')), f"{item['id']}.json")
            if url[0:5] != 's3://':
                url = f"s3://{bucket}/{url.lstrip('/')}"

            # add self link (and remove existing self link if present)
            item['links'] = [l for l in item['links'] if l['rel'] != 'self']
            item['links'].insert(0, {
                'rel': 'self',
                'href': url,
                'type': 'application/json'
            })

            # get s3 session
            s3session = get_s3_session(s3url=url)

            # publish to bucket
            headers = opts.get('headers', {})
            
            extra = {'ContentType': 'application/json'}
            extra.update(headers)
            s3session.upload_json(item, url, public=public, extra=extra)
            s3urls.append(url)
            logger.info(f"Uploaded STAC Item {item['id']} as {url}")

        return s3urls

    @classmethod
    def sns_attributes(self, item) -> Dict:
        """Create attributes from Item for publishing to SNS

        Args:
            item (Dict): A STAC Item

        Returns:
            Dict: Attributes for SNS publishing
        """
        attr = {
            'collection': {
                'DataType': 'String',
                'StringValue': item['collection']
            },
            'datetime': {
                'DataType': 'String',
                'StringValue': item['properties']['datetime']
            },
            'bbox.ll_lon': {
                'DataType': 'Number',
                'StringValue': str(item['bbox'][0])
            },
            'bbox.ll_lat': {
                'DataType': 'Number',
                'StringValue': str(item['bbox'][1])
            },
            'bbox.ur_lon': {
                'DataType': 'Number',
                'StringValue': str(item['bbox'][2])
            },
            'bbox.ur_lat': {
                'DataType': 'Number',
                'StringValue': str(item['bbox'][3])
            }     
        }
        if 'eo:cloud_cover' in item['properties']:
            attr['cloud_cover'] = {
                'DataType': 'Number',
                'StringValue': str(item['properties']['eo:cloud_cover'])
            }
        return attr

    def publish_to_sns(self, topic_arn=PUBLISH_TOPIC_ARN):
        """Publish this catalog to SNS

        Args:
            topic_arn (str, optional): ARN of SNS Topic. Defaults to PUBLISH_TOPIC_ARN.
        """
        for item in self['features']:
            logger.info(f"Publishing item {item['id']} to {topic_arn}")
            response = snsclient.publish(TopicArn=topic_arn, Message=json.dumps(item),
                                        MessageAttributes=self.sns_attributes(item))
            logger.debug(f"Response: {json.dumps(response)}")           

    def process(self) -> str:
        """Add this catalog to procesing queue

        Returns:
            str: Catalog ID
        """
        assert(CATALOG_BUCKET)
        assert(sqs_url)

        # create DynamoDB record - this will always overwrite any existing process
        try:
            statedb.create_item(self) 
        except Exception as err:
            msg = f"Error adding {self['id']} to database ({err})"
            logger.error(msg)
            return

        try:
            url = f"s3://{CATALOG_BUCKET}/{self['id']}/input.json"
            s3().upload_json(self, url)
            logger.debug(f"Uploaded {url}")
        except Exception as err:
            msg = f"Error adding {self['id']} input catalog to s3 ({err})"
            logger.error(msg)
            return

        try:
            response = sqsclient.send_message(
                QueueUrl = sqs_url,
                MessageBody = json.dumps(self)
            )
            # TODO - check response
            logger.info(f"Queued {self['id']}")
            return self['id']
        except Exception as err:
            msg = f"queue: error queuing {self['id']} ({err})"
            logger.error(msg)
            statedb.set_failed(self['id'], msg)    

        return self['id'] 


class Catalogs(object):

    def __init__(self, catalogs, state_items=None):
        self.catalogs = catalogs
        if state_items:
            assert(len(state_items) == len(self.catalogs))
        self.state_items = state_items

    def __getitem__(self, index):
        return self.catalogs[index]

    @property
    def catids(self) -> List[str]:
        """Return list of catalog IDs

        Returns:
            List[str]: List of Catalog IDs
        """
        return [c['id'] for c in self.catalogs]

    @classmethod
    def from_payload(cls, payload: Dict, **kwargs) -> Catalogs:
        """Parse a Cirrus payload and return a Catalogs instance

        Args:
            payload (Dict): A payload from SNS, SQS, or containing an s3 URL to payload

        Returns:
            Catalogs: A Catalogs instance
        """
        catalogs = []
        if 'Records' in payload:
            for record in [json.loads(r['body']) for r in payload['Records']]:
                if 'Message' in record:
                    # SNS
                    cat = Catalog(json.loads(record['Message']))
                    catalogs.append(cat)
                else:
                    # SQS
                    catalogs.append(Catalog(record))
        elif 'url' in payload:
            catalogs = [Catalog(s3().read_json(payload['url']))]
        elif 'Parameters' in payload and 'url' in payload['Parameters']:
            catalogs = [Catalog(s3().read_json(payload['Parameters']['url']))]
        else:
            catalogs = [Catalog(payload)]
        return cls(catalogs)

    @classmethod
    def from_catids(cls, catids: List[str], **kwargs) -> Catalogs:
        """Create Catalogs from list of Catalog IDs

        Args:
            catids (List[str]): List of catalog IDs

        Returns:
            Catalogs: A Catalogs instance
        """
        items = [statedb.dbitem_to_item(statedb.get_dbitem(catid)) for catid in catids]
        catalogs = []
        for item in items:
            cat = Catalog(s3().read_json(item['input_catalog']))
            catalogs.append(cat)
        logger.debug(f"Retrieved {len(catalogs)} from state db")
        return cls(catalogs, state_items=items)

    """
    @classmethod
    def from_statedb_paged(cls, collections, state, since: str=None, index: str='input_state', limit=None):
        catalogs = []
        # get first page
        resp = statedb.get_items_page(collections, state, since, index)
        for it in resp['items']:
            cat = Catalog(s3().read_json(it['input_catalog']))
            catalogs.append(cat)
        logger.debug(f"Retrieved {len(catalogs)} from state db")
        yield cls(catalogs, state_items=resp['items'])
        catalogs = []
        while 'nextkey' in resp:
            resp = statedb.get_items_page(collections, state, since, index, nextkey=resp['nextkey'])
            for it in resp['items']:
                cat = Catalog(s3().read_json(it['input_catalog']))
                catalogs.append(cat)
            logger.debug(f"Retrieved {len(catalogs)} from state db")
            yield cls(catalogs, state_items=resp['items'])
    """

    @classmethod
    def from_statedb(cls, collections, state, since: str=None, index: str='input_state', limit=None) -> Catalogs:
        """Create Catalogs object from set of StateDB Items

        Args:
            collections (str): String of collections (input or output depending on `index`)
            state (str): The state (QUEUED, PROCESSING, COMPLETED, FAILED, INVALID) of StateDB Items to get
            since (str, optional): Get Items since this duration ago (e.g., 10m, 8h, 1w). Defaults to None.
            index (str, optional): 'input_state' or 'output_state' Defaults to 'input_state'.
            limit ([type], optional): Max number of Items to return. Defaults to None.

        Returns:
            Catalogs: Catalogs instance
        """
        catalogs = []
        items = statedb.get_items(collections, state, since, index, limit=limit)
        logger.debug(f"Retrieved {len(items)} total items from statedb")
        for item in items:
            cat = Catalog(s3().read_json(item['input_catalog']))
            catalogs.append(cat)
        logger.debug(f"Retrieved {len(catalogs)} input catalogs")
        return cls(catalogs, state_items=items)

    def get_states(self):
        if self.state_items is None:
            items = [statedb.dbitem_to_item(i) for i in statedb.get_dbitems(self.catids)]
            self.state_items = items
        states = {c['catid']: c['state'] for c in self.state_items}
        return states

    def process(self, replace=False):
        """Create Item in Cirrus State DB for each Catalog and add to processing queue

        Args:
            catalog (Dict): A Cirrus Input Catalog
        """
        # check existing states            
        states = self.get_states()
        catalogs = []
        for cat in self.catalogs:
            # check existing state for Item, if any
            state = states.get(cat['id'], '')
            # don't try and process these - if they are stuck they should be removed from db
            #if state in ['QUEUED', 'PROCESSING']:
            #    logger.info(f"Skipping {cat['id']}, in {state} state")
            #    continue
            _replace = replace or cat['process'].get('replace', False)
            if state in ['FAILED', ''] or _replace:
                catalogs.append(cat)
            else:
                logger.info(f"Skipping {cat['id']}, in {state} state")
                continue

        # add to database, to s3, then to queue
        catids = []
        for catalog in catalogs:
            catids.append(catalog.process())
        return catids