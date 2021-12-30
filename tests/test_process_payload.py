import copy
import json
import pytest

from pathlib import Path

from cirrus.lib.process_payload import ProcessPayload

fixtures = Path(__file__).parent.joinpath('fixtures')


def read_json_fixture(filename):
    with fixtures.joinpath(filename).open() as f:
        return json.load(f)


@pytest.fixture()
def base_payload():
    return read_json_fixture('test-payload.json')


@pytest.fixture()
def sqs_event():
    return read_json_fixture('sqs-event.json')


def test_open_payload(base_payload):
    payload = ProcessPayload(**base_payload)
    assert payload['id'] == \
        "sentinel-s2-l2a/workflow-cog-archive/S2B_17HQD_20201103_0_L2A"


def test_update_payload(base_payload):
    del base_payload['id']
    del base_payload['features'][0]['links']
    payload = ProcessPayload(**base_payload, update=True)
    assert payload['id'] == \
        "sentinel-s2-l2a/workflow-cog-archive/S2B_17HQD_20201103_0_L2A"


def test_from_event(sqs_event):
    payload = ProcessPayload.from_event(sqs_event, update=True)
    assert len(payload['features']) == 1
    assert payload['id'] == \
        'sentinel-s2-l2a-aws/workflow-publish-sentinel/tiles-17-H-QD-2020-11-3-0'


def test_assign_collections(base_payload):
    payload = ProcessPayload(base_payload)
    payload['process']['output_options']['collections'] = {'test': '.*'}
    payload.assign_collections()
    assert payload['features'][0]['collection'] == 'test'


def test_sns_attributes(base_payload):
    payload = ProcessPayload(base_payload)
    attr = ProcessPayload.sns_attributes(payload['features'][0])
    assert attr['cloud_cover']['StringValue'] == '51.56'
    assert attr['datetime']['StringValue'] == '2020-11-03T15:22:26Z'


def test_get_items_by_properties(base_payload):
    base_payload['process']['item-queries'] = {
        'test': {'platform':'sentinel-2b'},
        'empty-test': {'platform': 'test-platform'}
    }
    payload = ProcessPayload.from_event(base_payload)
    assert payload.get_items_by_properties("test") == base_payload['features']
    assert payload.get_items_by_properties("empty-test") == []


def test_get_item_by_properties(base_payload):
    base_payload['process']['item-queries'] = {
        'feature1': {'platform':'sentinel-2b'},
        'feature2': {'platform': 'test-platform'}
    }
    feature1 = copy.deepcopy(base_payload['features'][0])
    feature2 = copy.deepcopy(base_payload['features'][0])
    feature2['properties']['platform'] = 'test-platform'
    base_payload['features'] = [feature1, feature2]
    payload = ProcessPayload.from_event(base_payload)
    assert payload.get_item_by_properties("feature1") == feature1
    assert payload.get_item_by_properties("feature2") == feature2


def test_next_payloads_no_list(base_payload):
    payloads = list(ProcessPayload.from_event(base_payload).next_payloads())
    assert len(payloads) == 0


def test_next_payloads_list_of_one(base_payload):
    base_payload['process'] = [base_payload['process']]
    payloads = list(ProcessPayload.from_event(base_payload).next_payloads())
    assert len(payloads) == 0


def test_next_payloads_list_of_four(base_payload):
    length = 4
    list_payload = copy.deepcopy(base_payload)
    list_payload['process'] = [base_payload['process']] * length

    # We should now have something like this:
    #
    # payload
    #   process:
    #     - wf1
    #     - wf2
    #     - wf3
    #     - wf4
    payloads = list(ProcessPayload.from_event(list_payload).next_payloads())

    # When we call next_payloads, we find one next payload (wf2)
    # with two to follow. So the length of the list returned should be
    # one, a process payload with a process array of length 3.
    assert len(payloads) == 1
    assert payloads[0]['process'] == [base_payload['process']] * (length-1)


def test_next_payloads_list_of_four_fork(base_payload):
    length = 3
    list_payload = copy.deepcopy(base_payload)
    list_payload['process'] = [base_payload['process']] * length
    list_payload['process'][1] = [base_payload['process']] * 2

    # We should now have something like this:
    #
    # payload
    #   process:
    #     - wf1
    #     - [ wf2a, wf2b]
    #     - wf3
    #     - wf4
    payloads = list(ProcessPayload.from_event(list_payload).next_payloads())

    # When we call next_payloads, we find two next payloads
    # (wf2a and wf2b), each with two to follow. So the length of
    # the list returned should be two, each a process payload
    # with a process array of length 3.
    assert len(payloads) == 2
    assert payloads[0]['process'] == [base_payload['process']] * (length-1)
    assert payloads[1]['process'] == [base_payload['process']] * (length-1)
