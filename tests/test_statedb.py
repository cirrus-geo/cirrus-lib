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
from cirrus.lib.statedb import StateDB, STATES

## fixtures
testpath = os.path.dirname(__file__)
table_name = 'cirrus-test-state'
test_dbitem = {
    'collections_workflow': 'col1_wf1',
    'itemids': 'item1/item2',
    'state_updated': f"QUEUED_{datetime.now()}",
    'created': datetime.now(),
    'updated': datetime.now()
}
test_item = {
    "id": "col1/workflow-wf1/item1/item2",
    "process": {
        "output_options": {
            "collections": {
                "output-collection": ".*"
            }
        }
    }
}


@mock_dynamodb2
def setup_table():
    boto3.setup_default_session()
    client = boto3.resource('dynamodb')
    with open(os.path.join(testpath, 'statedb_schema.json')) as f:
        schema = json.loads(f.read())
    table = client.create_table(**schema)
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    return StateDB(table_name)


TESTKEY = {
    'collections_workflow': 'col1_wf1',
    'itemids': 'item1/item2'
}


def test_payload_id_to_key():
    key = StateDB.payload_id_to_key(test_item['id'])
    assert(key['collections_workflow'] == "col1_wf1")
    assert(key['itemids'] == 'item1/item2')


def test_key_to_payload_id():
    payload_id = StateDB.key_to_payload_id(TESTKEY)
    assert(payload_id == test_item['id'])


def test_get_input_payload_url():
    url = StateDB.get_input_payload_url(TESTKEY)
    assert(f"{test_item['id']}/input.json" in url)


def test_dbitem_to_item():
    item = StateDB.dbitem_to_item(test_dbitem)
    assert(item['payload_id'] == test_item['id'])
    assert(item['workflow'] == 'wf1')
    assert(item['state'] == 'QUEUED')


def test_since_to_timedelta():
    td = StateDB.since_to_timedelta('1d')
    assert(td.days == 1)
    td = StateDB.since_to_timedelta('1h')
    assert(td.seconds == 3600)
    td = StateDB.since_to_timedelta('10m')
    assert(td.seconds == 600)


NITEMS = 1000


@pytest.fixture(scope='session')
def state_table():
    mock = mock_dynamodb2()
    mock.start()
    statedb = setup_table()
    for i in range(NITEMS):
        newitem = deepcopy(test_item)
        statedb.set_processing(
            f'{newitem["id"]}{i}',
            execution='arn::test',
        )
    statedb.set_processing(
        f'{test_item["id"]}_processing',
        execution='arn::test',
    )
    statedb.set_completed(
        f'{test_item["id"]}_completed',
        outputs=['item1', 'item2'],
    )
    statedb.set_failed(
        f'{test_item["id"]}_failed',
        'failed',
    )
    statedb.set_invalid(
        f'{test_item["id"]}_invalid',
        'invalid',
    )
    statedb.set_aborted(
        f'{test_item["id"]}_aborted',
    )
    yield statedb
    for i in range(NITEMS):
        statedb.delete_item(f'{test_item["id"]}{i}')
    for s in STATES:
        statedb.delete_item(f'{test_item["id"]}_{s.lower()}')
    statedb.delete()
    mock.stop()


def test_get_items(state_table):
    items = state_table.get_items(
        test_dbitem['collections_workflow'],
        state='PROCESSING',
        since='1h',
    )
    assert(len(items) == NITEMS + 1)
    items = state_table.get_items(
        test_dbitem['collections_workflow'],
        state='PROCESSING',
        since='1h',
        limit=1,
    )
    assert(len(items) == 1)


def test_get_dbitem(state_table):
    dbitem = state_table.get_dbitem(test_item['id'] + '0')
    assert(dbitem['itemids'] == test_dbitem['itemids'] + '0')
    assert(dbitem['collections_workflow'] == test_dbitem['collections_workflow'])
    assert(dbitem['state_updated'].startswith('PROCESSING'))


def test_get_dbitem_noitem(state_table):
    dbitem = state_table.get_dbitem('no-collection/workflow-none/fake-id')
    assert(dbitem is None)


def test_get_dbitems(state_table):
    ids = [test_item['id'] + str(i) for i in range(10)]
    dbitems = state_table.get_dbitems(ids)
    assert(len(dbitems) == len(ids))
    for dbitem in dbitems:
        assert(state_table.key_to_payload_id(dbitem) in ids)


def test_get_dbitems_duplicates(state_table):
    ids = [test_item['id'] + str(i) for i in range(10)]
    ids.append(ids[0])
    dbitems = state_table.get_dbitems(ids)
    for dbitem in dbitems:
        assert(state_table.key_to_payload_id(dbitem) in ids)


def test_get_dbitems_noitems(state_table):
    dbitems = state_table.get_dbitems(['no-collection/workflow-none/fake-id'])
    assert(len(dbitems) == 0)


def test_get_state(state_table):
    for s in STATES:
        state = state_table.get_state(test_item['id'] + f"_{s.lower()}")
        assert(state == s)
    state = state_table.get_state(test_item['id'] + 'nosuchitem')


def test_get_states(state_table):
    ids = [test_item['id'] + f"_{s.lower()}" for s in STATES]
    states = state_table.get_states(ids)
    assert(len(ids) == len(states))
    for i, id in enumerate(ids):
        assert(states[id] == STATES[i])


def test_get_counts(state_table):
    count = state_table.get_counts(test_dbitem['collections_workflow'])
    assert(count == NITEMS + len(STATES))
    for s in STATES:
        count = state_table.get_counts(test_dbitem['collections_workflow'], state=s)
        if s == 'PROCESSING':
            assert(count == NITEMS + 1)
        else:
            assert(count == 1)
    count = state_table.get_counts(test_dbitem['collections_workflow'], since='1h')


def test_set_processing(state_table):
    resp = state_table.set_processing(test_item['id'], execution='arn::test1')
    assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
    dbitem = state_table.get_dbitem(test_item['id'])
    assert(StateDB.key_to_payload_id(dbitem) == test_item['id'])
    assert(dbitem['executions'] == ['arn::test1'])


def test_second_execution(state_table):
    # check that processing adds new execution to list
    resp = state_table.set_processing(test_item['id'], execution='arn::test2')
    dbitem = state_table.get_dbitem(test_item['id'])
    assert(len(dbitem['executions']) == 2)
    assert(dbitem['executions'][-1] == 'arn::test2')


def test_set_outputs(state_table):
    resp = state_table.set_completed(test_item['id'], outputs=['output-item'])
    assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
    dbitem = state_table.get_dbitem(test_item['id'])
    assert(dbitem['outputs'][0] == 'output-item')


def test_set_completed(state_table):
    resp = state_table.set_completed(test_item['id'])
    assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
    dbitem = state_table.get_dbitem(test_item['id'])
    assert(dbitem['state_updated'].startswith('COMPLETED'))


def test_set_failed(state_table):
    resp = state_table.set_failed(test_item['id'], msg='test failure')
    assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
    dbitem = state_table.get_dbitem(test_item['id'])
    assert(dbitem['state_updated'].startswith('FAILED'))
    assert(dbitem['last_error'] == 'test failure')


def test_set_completed_with_outputs(state_table):
    resp = state_table.set_completed(test_item['id'], outputs=['output-item2'])
    assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
    dbitem = state_table.get_dbitem(test_item['id'])
    assert(dbitem['state_updated'].startswith('COMPLETED'))
    assert(dbitem['outputs'][0] == 'output-item2')


def test_set_invalid(state_table):
    resp = state_table.set_invalid(test_item['id'], msg='test failure')
    assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
    dbitem = state_table.get_dbitem(test_item['id'])
    assert(dbitem['state_updated'].startswith('INVALID'))
    assert(dbitem['last_error'] == 'test failure')


def test_set_aborted(state_table):
    resp = state_table.set_aborted(test_item['id'])
    assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)
    dbitem = state_table.get_dbitem(test_item['id'])
    assert(dbitem['state_updated'].startswith('ABORTED'))


def test_delete_item(state_table):
    state_table.delete_item(test_item['id'])
    dbitem = state_table.get_dbitem(test_item['id'])
    assert(dbitem is None)


def _test_get_counts_paging(state_table):
    for i in range(5000):
        state_table.set_processing(test_item['id'] + f"_{i}", execution='arn::test')
    count = state_table.get_counts(test_dbitem['collections_workflow'])
    assert(count == 1004)
    for i in range(5000):
        state_table.delete_item(test_item['id'] + f"_{i}")
