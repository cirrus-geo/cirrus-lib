from __future__ import annotations

import boto3
import json
import logging
import os
import re
import uuid
import warnings
import jsonpath_ng.ext as jsonpath

from datetime import datetime, timezone
from typing import Dict, Optional, List
from copy import deepcopy

from boto3utils import s3
from cirrus.lib.statedb import StateDB
from cirrus.lib.logging import get_task_logger
from cirrus.lib.transfer import get_s3_session
from cirrus.lib.utils import get_path, property_match


# envvars
PAYLOAD_BUCKET = os.getenv('CIRRUS_PAYLOAD_BUCKET', None)
PUBLISH_TOPIC_ARN = os.getenv('CIRRUS_PUBLISH_TOPIC_ARN', None)

# clients
statedb = StateDB()
snsclient = boto3.client('sns')
stepfunctions = boto3.client('stepfunctions')

# logging
logger = logging.getLogger(__name__)


class ProcessPayload(dict):

    def __init__(self, *args, update=False, state_item=None, **kwargs):
        """Initialize a ProcessPayload, verify required fields, and assign an ID

        Args:
            state_item (Dict, optional): Dictionary of entry in StateDB. Defaults to None.
        """
        super().__init__(*args, **kwargs)

        self.logger = get_task_logger(__name__, payload=self)

        # validate process block
        # TODO: assert isn't safe for this use if debug is off
        assert(self['type'] == 'FeatureCollection')
        assert('process' in self)

        self.process = (
            self['process'][0]
            if isinstance(self['process'], list)
            else self['process']
        )

        if update:
            self.update()

        if 'output_options' in self.process and not 'upload_options' in self.process:
            self.process['upload_options'] = self.process['output_options']
            warnings.warn(
                "Deprecated: process 'output_options' has been renamed to 'upload_options'",
            )

        # We could explicitly handle the situation where both output and upload
        # options are provided, but I think it reasonable for us to expect some
        # people might continue using it where they had been (ab)using it for
        # custom needs, which is why we don't just pop it above. In fact,
        # because we are copying and not moving the values to that new key, we
        # are creating this exact situation.

        assert('upload_options' in self.process)
        assert('workflow' in self.process)

        # convert old functions field to tasks
        if 'functions' in self.process:
            warnings.warn("Deprecated: process 'functions' has been renamed to 'tasks'")
            self.process['tasks'] = self.process.pop('functions')

        assert('tasks' in self.process)
        self.tasks = self.process['tasks']

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

    @classmethod
    def from_event(cls, event: Dict, **kwargs) -> ProcessPayload:
        """Parse a Cirrus event and return a ProcessPayload instance

        Args:
            event (Dict): An event from SNS, SQS, or containing an s3 URL to payload

        Returns:
            ProcessPayload: A ProcessPaylaod instance
        """
        if 'Records' in event:
            records = [json.loads(r['body']) for r in event['Records']]
            # there should be only one
            assert(len(records) == 1)
            if 'Message' in records[0]:
                # SNS
                payload = json.loads(records[0]['Message'])
            else:
                # SQS
                payload = records[0]
        elif 'url' in event:
            payload = s3().read_json(event['url'])
        elif 'Parameters' in event and 'url' in event['Parameters']:
            # this is Batch, get the output payload
            url = event['Parameters']['url'].replace('.json', '_out.json')
            payload = s3().read_json(url)
        else:
            payload = event
        return cls(payload, **kwargs)

    def get_task(self, task_name, *args, **kwargs):
        return self.tasks.get(task_name, *args, **kwargs)

    def next_payloads(self):
        if isinstance(self['process'], dict) or len(self['process']) <= 1:
            return None
        next_processes = (
            [self['process'][1]]
            if isinstance(self['process'][1], dict)
            else self['process'][1]
        )
        for process in next_processes:
            new = deepcopy(self)
            del new['id']
            new['process'].pop(0)
            new['process'][0] = process
            if 'chain_filter' in process:
                jsonfilter = jsonpath.parse(
                    f'$.features[?({process["chain_filter"]})]',
                )
                new['features'] = [
                    match.value for match in jsonfilter.find(new)
                ]
            yield new

    def update(self):
        if 'collections' in self.process:
            # allow overriding of collections name
            collections_str = self.process['collections']
        else:
            # otherwise, get from items
            cols = sorted(list(set([i['collection'] for i in self['features'] if 'collection' in i])))
            input_collections = cols if len(cols) != 0 else 'none'
            collections_str = '/'.join(input_collections)

        items_str = '/'.join(sorted(list([i['id'] for i in self['features']])))
        if 'id' not in self:
            self['id'] = f"{collections_str}/workflow-{self.process['workflow']}/{items_str}"

    # assign collections to Items given a mapping of Col ID: ID regex
    def assign_collections(self):
        """Assign new collections to all Items (features) in ProcessPayload
            based on self.process['upload_options']['collections']
        """
        collections = self.process['upload_options'].get('collections', {})
        # loop through all Items in ProcessPayload
        for item in self['features']:
            # loop through all provided output collections regexs
            for col in collections:
                regex = re.compile(collections[col])
                if regex.match(item['id']):
                    self.logger.debug(f"Setting collection to {col}")
                    item['collection'] = col

    def get_payload(self) -> Dict:
        """Get original payload for this ProcessPayload

        Returns:
            Dict: Cirrus Input ProcessPayload
        """
        payload = json.dumps(self)
        if PAYLOAD_BUCKET and len(payload.encode('utf-8')) > 30000:
            assert(PAYLOAD_BUCKET)
            url = f"s3://{PAYLOAD_BUCKET}/payloads/{uuid.uuid1()}.json"
            s3().upload_json(self, url)
            return {'url': url}
        else:
            return dict(self)

    def get_items_by_properties(self, key):
        properties = self.process['item-queries'].get(key, {})
        features = []
        if properties:
            for feature in self['features']:
                if property_match(feature, properties):
                    features.append(feature)
        else:
            msg = 'unable to find item, please check properties parameters'
            logger.error(msg)
            raise Exception(msg)
        return features

    def get_item_by_properties(self, key):
        features = self.get_items_by_properties(key)
        if len(features) == 1:
            return features[0]
        elif len(features) > 1:
            msg = (
                'multiple items returned, '
                'please check properties parameters, '
                'or use get_items_by_properties'
            )
            logger.error(msg)
            raise Exception(msg)
        else:
            return None


    # publish the items in this ProcessPayload
    def publish_items_to_s3(self, bucket, public=False) -> List:
        """Publish all Items to s3

        Args:
            bucket (str): Name of bucket to publish to
            public (bool, optional): Make published STAC Item public. Defaults to False.

        Returns:
            List: List of s3 URLs to published Items
        """
        opts = self.process.get('upload_options', {})
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

            # if existing item use created date
            now = datetime.now(timezone.utc).isoformat()
            created = None
            if s3session.exists(url):
                old_item = s3session.read_json(url)
                created = old_item['properties'].get('created', None)
            if created is None:
                created = now
            item['properties']['created'] = created
            item['properties']['updated'] = now

            # publish to bucket
            headers = opts.get('headers', {})

            extra = {'ContentType': 'application/json'}
            extra.update(headers)
            s3session.upload_json(item, url, public=public, extra=extra)
            s3urls.append(url)
            self.logger.info("Published to s3")

        return s3urls

    @staticmethod
    def sns_attributes(item) -> Dict:
        """Create attributes from Item for publishing to SNS

        Args:
            item (Dict): A STAC Item

        Returns:
            Dict: Attributes for SNS publishing
        """
        # note that sns -> sqs supports only 10 message attributes
        # when not using raw mode, and we currently have 10 attrs
        # possible
        attr = {
            'collection': {
                'DataType': 'String',
                'StringValue': item['collection']
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

        if 'start_datetime' in item['properties']:
            attr['start_datetime'] = {
                'DataType': 'String',
                'StringValue': item['properties']['start_datetime']
            }
        elif 'datetime' in item['properties']:
            attr['start_datetime'] = {
                'DataType': 'String',
                'StringValue': item['properties']['datetime']
            }

        if 'end_datetime' in item['properties']:
            attr['end_datetime'] = {
                'DataType': 'String',
                'StringValue': item['properties']['end_datetime']
            }
        elif 'datetime' in item['properties']:
            attr['end_datetime'] = {
                'DataType': 'String',
                'StringValue': item['properties']['datetime']
            }

        if 'datetime' in item['properties']:
            attr['datetime'] = {
                'DataType': 'String',
                'StringValue': item['properties']['datetime']
            }

        if 'eo:cloud_cover' in item['properties']:
            attr['cloud_cover'] = {
                'DataType': 'Number',
                'StringValue': str(item['properties']['eo:cloud_cover'])
            }

        if item['properties']['created'] != item['properties']['updated']:
            attr['status'] = {
                'DataType': 'String',
                'StringValue': 'updated'
            }
        else:
            attr['status'] = {
                'DataType': 'String',
                'StringValue': 'created'
            }

        return attr

    def publish_to_sns(self, topic_arn):
        """Publish this ProcessPayload to SNS

        Args:
            topic_arn (str): ARN of SNS Topic.
        """
        response = snsclient.publish(
            TopicArn=topic_arn,
            Message=json.dumps(self),
        )
        self.logger.debug(f"Published ProcessPayload to {topic_arn}")
        return response

    def publish_items_to_sns(self, topic_arn=PUBLISH_TOPIC_ARN):
        """Publish this ProcessPayload's items to SNS

        Args:
            topic_arn (str, optional): ARN of SNS Topic. Defaults to PUBLISH_TOPIC_ARN.
        """
        responses = []
        for item in self['features']:
            responses.append(snsclient.publish(
                TopicArn=topic_arn,
                Message=json.dumps(item),
                MessageAttributes=self.sns_attributes(item),
            ))
            self.logger.debug(f"Published item to {topic_arn}")
        return responses

    def __call__(self) -> str:
        """Add this ProcessPayload to Cirrus and start workflow

        Returns:
            str: ProcessPayload ID
        """
        assert(PAYLOAD_BUCKET)

        arn = os.getenv('CIRRUS_BASE_WORKFLOW_ARN') + self.process['workflow']

        # start workflow
        try:
            # add input payload to s3
            url = f"s3://{PAYLOAD_BUCKET}/{self['id']}/input.json"
            s3().upload_json(self, url)

            # create DynamoDB record - this overwrites existing states other than PROCESSING
            resp = statedb.claim_processing(self['id'])

            # invoke step function
            self.logger.debug(f"Running Step Function {arn}")
            exe_response = stepfunctions.start_execution(stateMachineArn=arn, input=json.dumps(self.get_payload()))

            # add execution to DynamoDB record
            resp = statedb.set_processing(self['id'], exe_response['executionArn'])

            return self['id']
        except statedb.db.meta.client.exceptions.ConditionalCheckFailedException:
            self.logger.warning('Already in PROCESSING state')
            return None
        except Exception as err:
            msg = f"failed starting workflow ({err})"
            self.logger.exception(msg)
            statedb.set_failed(self['id'], msg)
            raise


class ProcessPayloads(object):

    def __init__(self, process_payloads, state_items=None):
        self.payloads = process_payloads
        if state_items:
            assert(len(state_items) == len(self.payloads))
        self.state_items = state_items

    def __getitem__(self, index):
        return self.payloads[index]

    @property
    def payload_ids(self) -> List[str]:
        """Return list of Payload IDs

        Returns:
            List[str]: List of Payload IDs
        """
        return [c['id'] for c in self.payloads]

    @classmethod
    def from_payload_ids(cls, payload_ids: List[str], **kwargs) -> ProcessPayloads:
        """Create ProcessPayloads from list of Payload IDs

        Args:
            payload_ids (List[str]): List of Payload IDs

        Returns:
            ProcessPayloads: A ProcessPayloads instance
        """
        items = [statedb.dbitem_to_item(statedb.get_dbitem(payload_id)) for payload_id in payload_ids]
        payloads = []
        for item in items:
            payload = ProcessPayload(s3().read_json(item['payload']))
            payloads.append(payload)
        logger.debug(f"Retrieved {len(payloads)} from state db")
        return cls(payloads, state_items=items)

    """
    @classmethod
    def from_statedb_paged(cls, collections, state, since: str=None, index: str='input_state', limit=None):
        payloads = []
        # get first page
        resp = statedb.get_items_page(collections, state, since, index)
        for it in resp['items']:
            payload = ProcessPayload(s3().read_json(it['input_catalog']))
            payloads.append(payload)
        self.logger.debug(f"Retrieved {len(payloads)} from state db")
        yield cls(payloads, state_items=resp['items'])
        payloads = []
        while 'nextkey' in resp:
            resp = statedb.get_items_page(collections, state, since, index, nextkey=resp['nextkey'])
            for it in resp['items']:
                payload = ProcessPayload(s3().read_json(it['input_catalog']))
                payloads.append(payload)
            self.logger.debug(f"Retrieved {len(payloads)} from state db")
            yield cls(payloads, state_items=resp['items'])
    """

    @classmethod
    def from_statedb(cls, collections, state, since: str=None, index: str='input_state', limit=None) -> ProcessPayloads:
        """Create ProcessPayloads object from set of StateDB Items

        Args:
            collections (str): String of collections (input or output depending on `index`)
            state (str): The state (QUEUED, PROCESSING, COMPLETED, FAILED, INVALID, ABORTED) of StateDB Items to get
            since (str, optional): Get Items since this duration ago (e.g., 10m, 8h, 1w). Defaults to None.
            index (str, optional): 'input_state' or 'output_state' Defaults to 'input_state'.
            limit ([type], optional): Max number of Items to return. Defaults to None.

        Returns:
            ProcessPayloads: ProcessPayloads instance
        """
        payloads = []
        items = statedb.get_items(collections, state, since, index, limit=limit)
        logger.debug(f"Retrieved {len(items)} total items from statedb")
        for item in items:
            payload = ProcessPayload(s3().read_json(item['payload']))
            payloads.append(payload)
        logger.debug(f"Retrieved {len(payloads)} process payloads")
        return cls(payloads, state_items=items)

    def get_states(self):
        if self.state_items is None:
            items = [statedb.dbitem_to_item(i) for i in statedb.get_dbitems(self.payload_ids)]
            self.state_items = items
        states = {c['payload_id']: c['state'] for c in self.state_items}
        return states

    def process(self, replace=False):
        """Create Item in Cirrus State DB for each ProcessPayload and add to processing queue
        """
        payload_ids = []
        # check existing states
        states = self.get_states()
        for payload in self.payloads:
            _replace = replace or payload.process.get('replace', False)
            # check existing state for Item, if any
            state = states.get(payload['id'], '')
            # don't try and process these - if they are stuck they should be removed from db
            #if state in ['QUEUED', 'PROCESSING']:
            #    logger.info(f"Skipping {payload['id']}, in {state} state")
            #    continue
            if payload['id'] in payload_ids:
                logger.warning(f"Dropping duplicated payload {payload['id']}")
            elif state in ['FAILED', 'ABORTED', ''] or _replace:
                payload_id = payload()
                if payload_id is not None:
                    payload_ids.append(payload_id)
            else:
                logger.info(f"Skipping {payload['id']}, input already in {state} state")
                continue

        return payload_ids
