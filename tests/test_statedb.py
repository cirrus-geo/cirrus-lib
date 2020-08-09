# import moto before any boto3 module
from moto import mock_dynamodb2
import boto3
import inspect
import json
import os
import pytest
import unittest

from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from cirruslib.statedb import StateDB, STATES


## fixtures
testpath = os.path.dirname(__file__)
table_name = 'cirrus-test-state'

test_item = {
    "id": "input-collection/workflow-test/item",
    "process": {
        "output_options": {
            "collections": {
                "output-collection": ".*"
            }
        }
    }
}

test_dbitem = {
    'input_collections': 'input-collection',
    'output_collections': 'output-collection',
    'id': 'test/item',
    'current_state': f"QUEUED_{datetime.now()}",
    'created_at': datetime.now()
}


def setUpModule():
    client = boto3.resource('dynamodb')
    with open(os.path.join(testpath, 'statedb_schema.json')) as f:
        schema = json.loads(f.read())
    table = client.create_table(**schema)
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)


def tearDownModule():
    client = boto3.resource('dynamodb')
    table = client.Table(table_name)
    table.delete()
    table.wait_until_not_exists()


class TestClassMethods(unittest.TestCase):

    testkey = {
        'input_collections': 'input-collection',
        'id': 'test/item'
    }

    def test_catid_to_key(self):
        key = StateDB.catid_to_key(test_item['id'])
        assert(key['input_collections'] == "input-collection")
        assert(key['id'] == 'test/item')

    def test_key_to_catid(self):
        catid = StateDB.key_to_catid(self.testkey)
        assert(catid == test_item['id'])

    def test_get_input_catalog_url(self):
        url = StateDB.get_input_catalog_url(self.testkey)
        assert(f"{test_item['id']}/input.json" in url)

    def test_dbitem_to_item(self):
        item = StateDB.dbitem_to_item(test_dbitem)
        assert(item['catid'] == test_item['id'])
        assert(item['workflow'] == 'test')
        assert(item['state'] == 'QUEUED')

    def test_since_to_timedelta(self):
        td = StateDB.since_to_timedelta('1d')
        assert(td.days == 1)
        td = StateDB.since_to_timedelta('1h')
        assert(td.seconds == 3600)
        td = StateDB.since_to_timedelta('10m')
        assert(td.seconds == 600)


# TODO - figure out why mocking still sends queries to AWS
#@mock_dynamodb2
class TestDbItems(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.statedb = StateDB(table_name)
        for i in range(10):
            newitem = deepcopy(test_item)
            newitem['id'] = newitem['id'] + str(i)
            cls.statedb.create_item(newitem)

    @classmethod
    def tearDownClass(cls):
        for i in range(10):
            cls.statedb.delete_item(test_item['id'] + str(i))

    def test_create_item(self):
        resp = self.statedb.create_item(test_item)
        assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
        dbitem = self.statedb.get_dbitem(test_item['id'])
        assert(dbitem['id'] == test_dbitem['id'])
        self.statedb.delete_item(test_item['id'])
        #with self.assertRaises(Exception):
        #    dbitem = self.statedb.get_dbitem(test_item['id'])

    def test_get_dbitem(self):
        item = self.statedb.get_dbitem(test_item['id'] + '0')
        assert(item['id'] == test_dbitem['id'] + '0')
        assert(item['input_collections'] == test_dbitem['input_collections'])
        assert(item['current_state'].startswith('QUEUED'))

    def test_get_dbitems(self):
        ids = [test_item['id'] + str(i) for i in range(10)]
        dbitems = self.statedb.get_dbitems(ids)
        assert(len(dbitems) == len(ids))
        for dbitem in dbitems:
            assert(self.statedb.key_to_catid(dbitem) in ids)


class TestStates(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.statedb = StateDB(table_name)
        cls.statedb.create_item(test_item, state='QUEUED')
        for i, s in enumerate(STATES):
            newitem = deepcopy(test_item)
            newitem['id'] = newitem['id'] + str(i)
            cls.statedb.create_item(newitem, state=s)

    @classmethod
    def tearDownClass(cls):
        cls.statedb.delete_item(test_item['id'])
        for i in range(len(STATES)):
            cls.statedb.delete_item(test_item['id'] + str(i))        

    def test_get_state(self):
        for i, s in enumerate(STATES):
            state = self.statedb.get_state(test_item['id'] + str(i))
            assert(state == s)

    def test_get_states(self):
        ids = [test_item['id'] + str(i) for i in range(len(STATES))]
        states = self.statedb.get_states(ids)
        assert(len(ids) == len(states))
        for i, id in enumerate(ids):
            assert(states[id] == STATES[i])

    def test_set_processing(self):
        resp = self.statedb.set_processing(test_item['id'], execution='testarn')
        assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
        dbitem = self.statedb.get_dbitem(test_item['id'])
        assert(dbitem['current_state'].startswith('PROCESSING'))
        assert(dbitem['execution'] == 'testarn')

    def test_set_complete(self):
        resp = self.statedb.set_completed(test_item['id'], urls=['output-url'])
        assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
        dbitem = self.statedb.get_dbitem(test_item['id'])
        assert(dbitem['current_state'].startswith('COMPLETED'))
        assert(dbitem['output_urls'][0] == 'output-url')

    def test_set_failed(self):
        resp = self.statedb.set_failed(test_item['id'], msg='test failure')
        assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
        dbitem = self.statedb.get_dbitem(test_item['id'])
        assert(dbitem['current_state'].startswith('FAILED'))

    def test_set_invalid(self):
        resp = self.statedb.set_invalid(test_item['id'], msg='test failure')
        assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
        dbitem = self.statedb.get_dbitem(test_item['id'])
        assert(dbitem['current_state'].startswith('INVALID'))

# TODO - figure out why mocking still sends queries to AWS
#@mock_dynamodb2
class TestItems(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.statedb = StateDB(table_name)
        cls.statedb.create_item(test_item, state='QUEUED')
        for i, s in enumerate(STATES):
            newitem = deepcopy(test_item)
            newitem['id'] = newitem['id'] + str(i)
            cls.statedb.create_item(newitem, state=s)

    @classmethod
    def tearDownClass(cls):
        cls.statedb.delete_item(test_item['id'])
        for i in range(len(STATES)):
            cls.statedb.delete_item(test_item['id'] + str(i))     