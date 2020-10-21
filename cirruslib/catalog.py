from __future__ import annotations

import boto3
import json
import logging
import os
import re
import uuid
from traceback import format_exc
from typing import Dict, Optional, List

from boto3utils import s3
from cirruslib.statedb import StateDB
from cirruslib.logging import DynamicLoggerAdapter
from cirruslib.transfer import get_s3_session
from cirruslib.utils import get_path
from pythonjsonlogger import jsonlogger

logger = logging.getLogger(__name__)


# envvars
DATA_BUCKET = os.getenv('CIRRUS_DATA_BUCKET', None)
CATALOG_BUCKET = os.getenv('CIRRUS_CATALOG_BUCKET', None)
PUBLISH_TOPIC_ARN = os.getenv('CIRRUS_PUBLISH_TOPIC_ARN', None)

# clients
statedb = StateDB()
snsclient = boto3.client('sns')
stepfunctions = boto3.client('stepfunctions')


class Catalog(dict):

    def __init__(self, *args, update=False, state_item=None, **kwargs):
        """Initialize a Catalog, verify required fields, and assign an ID

        Args:
            state_item (Dict, optional): Dictionary of entry in StateDB. Defaults to None.
        """
        super(Catalog, self).__init__(*args, **kwargs)

        self.logger = DynamicLoggerAdapter(logger, self, keys=['id', 'stac_version'])

        # convert old functions field to tasks
        if 'functions' in self['process']:
            self['process']['tasks'] = self['process'].pop('functions')

        if update:
            self.update()

        # validate process block
        assert(self['type'] == 'FeatureCollection')
        assert('process' in self)
        assert('output_options' in self['process'])
        assert('workflow' in self['process'])
        assert('tasks' in self['process'])
        assert('workflow-' in self['id'])

        # TODO - validate with a JSON schema
        #if schema:
        #    pass
        # For now, just make check that there is at least one item
        assert(len(self['features']) > 0)
        for item in self['features']:
            if 'links' not in item:
                item['links'] = [] 

        # update collection IDs of member Items
        self.assign_collections()

        self.state_item = state_item

    def update(self):

        # Input collections
        if 'input_collections' not in self['process']:
            cols = sorted(list(set([i['collection'] for i in self['features'] if 'collection' in i])))
            self['process']['input_collections'] = cols if len(cols) != 0 else 'none'

        # generate ID
        collections_str = '/'.join(self['process']['input_collections'])
        items_str = '/'.join(sorted(list([i['id'] for i in self['features']])))
        if 'id' not in self:
            self['id'] = f"{collections_str}/workflow-{self['process']['workflow']}/{items_str}"

        logger.debug(f"Catalog after validate_and_update: {json.dumps(self)}")


    # assign collections to Items given a mapping of Col ID: ID regex
    def assign_collections(self):
        """Assign new collections to all Items (features) in Catalog
            based on self['process']['output_options']['collections']
        """
        collections = self['process']['output_options'].get('collections', {})
        # loop through all Items in Catalog
        for item in self['features']:
            # loop through all provided output collections regexs
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
            if public:
                url = s3.s3_to_https(url)

            # add canonical and self links (and remove existing self link if present)
            item['links'] = [l for l in item['links'] if l['rel'] not in ['self', 'canonical']]
            item['links'].insert(0, {
                'rel': 'canonical',
                'href': url,
                'type': 'application/json'
            })
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
            logger.debug(f"Publishing item {item['id']} to {topic_arn}")
            response = snsclient.publish(TopicArn=topic_arn, Message=json.dumps(item),
                                        MessageAttributes=self.sns_attributes(item))         

    def process(self) -> str:
        """Add this Catalog to Cirrus and start workflow

        Returns:
            str: Catalog ID
        """
        assert(CATALOG_BUCKET)

        # start workflow
        try:
            # add input catalog to s3
            url = f"s3://{CATALOG_BUCKET}/{self['id']}/input.json"
            s3().upload_json(self, url)
            logger.debug(f"Uploaded {url}")

            # invoke step function
            arn = os.getenv('BASE_WORKFLOW_ARN') + self['process']['workflow']
            logger.debug(f"Running {arn} on {self['id']}")
            exe_response = stepfunctions.start_execution(stateMachineArn=arn, input=json.dumps(self.get_payload()))

            # create DynamoDB record - this will always overwrite any existing process
            resp = statedb.add_item(self, exe_response['executionArn'])
            
            return self['id']
        except Exception as err:
            msg = f"process: failed starting {self['id']} ({err})"
            logger.error(msg)
            logger.error(format_exc())
            statedb.add_failed_item(self, msg)
            raise err


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
            # this is Batch, get the output payload
            url = payload['Parameters']['url'].replace('.json', '_out.json')
            catalogs = [Catalog(s3().read_json(url))]
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
        catids = []
        # check existing states            
        states = self.get_states()
        for cat in self.catalogs:
            _replace = replace or cat['process'].get('replace', False)
            # check existing state for Item, if any
            state = states.get(cat['id'], '')
            # don't try and process these - if they are stuck they should be removed from db
            #if state in ['QUEUED', 'PROCESSING']:
            #    logger.info(f"Skipping {cat['id']}, in {state} state")
            #    continue
            if state in ['FAILED', ''] or _replace:
                catids.append(cat.process())
            else:
                logger.info(f"Skipping {cat['id']}, in {state} state")
                continue

        return catids
