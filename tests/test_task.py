#!/usr/bin/env python
import json
from pathlib import Path

import vcr

from cirrus.lib.task import Task


testpath = Path(__file__).parent
cassettepath = testpath / 'fixtures' / 'cassettes'


class NothingTask(Task):
    _name = 'nothing-task'
    _description = 'this task does nothing'

    def process(self):
        return self.items


class DerivedItemTask(Task):
    _name = 'derived-item-task'
    _description = 'this task creates a dervied item'

    def process(self):
        return [self.create_item_from_item(self.items[0])]


def get_test_payload(name='test-payload'):
    filename = testpath / "fixtures" / f"{name}.json"
    with open(filename) as f:
        payload = json.loads(f.read())
    return payload


def test_task_init():
    payload = get_test_payload()
    t = NothingTask(payload)
    assert(t.id)
    assert(len(t.original_items) == 1)
    assert(len(t.items) == 1)
    assert(t._local == False)
    assert(t.logger.name == 'task.nothing-task')


def test_edit_payload():
    payload = get_test_payload()
    t = NothingTask(payload)
    t.process_definition['workflow'] = 'test-task-workflow'
    assert(t._payload['process']['workflow'] == 'test-task-workflow')


def test_edit_items():
    payload = get_test_payload()
    t = NothingTask(payload)
    t.items[0]['id'] = 'test-task'
    assert(t._payload['features'][0]['id'] == 'test-task')


def test_tmp_workdir():
    t = NothingTask(get_test_payload())
    assert(t._tmpworkdir == True)
    workdir = t._workdir
    assert(workdir.parts[1] == 'tmp')
    assert(workdir.parts[2].startswith('tmp'))
    assert(workdir.is_dir() == True)
    del t
    assert(workdir.is_dir() == False)


def test_workdir():
    t = NothingTask(get_test_payload(), workdir = testpath / 'test_task')
    assert(t._tmpworkdir == False)
    workdir = t._workdir
    assert(workdir.parts[-1] == 'test_task')
    assert(workdir.is_dir() == True)
    del t
    assert(workdir.is_dir() == True)    
    workdir.rmdir()
    assert(workdir.is_dir() == False)


def test_parameters():
    payload = get_test_payload()
    t = NothingTask(payload)
    assert(t.id == payload['id'])
    assert(t.process_definition['workflow'] == 'cog-archive')
    assert(t.output_options['path_template'] == payload['process']['output_options']['path_template'])


def test_process():
    payload = get_test_payload()
    t = NothingTask(payload)
    items = t.process()
    assert(items[0]['id'] == payload['features'][0]['id'])


def test_derived_item():
    t = DerivedItemTask(get_test_payload())
    t.items = t.process()
    links = [l for l in t.items[0]['links'] if l['rel'] == 'derived_from']
    assert(len(links) == 1)
    self_link = [l for l in t.items[0]['links'] if l['rel'] == 'self'][0]
    assert(links[0]['href'] == self_link['href'])


def test_task_handler():
    payload = get_test_payload()
    self_link = [l for l in payload['features'][0]['links'] if l['rel'] == 'self'][0]
    output_payload = DerivedItemTask.handler(payload)
    derived_link = [l for l in output_payload['features'][0]['links'] if l['rel'] == 'derived_from'][0]
    assert(derived_link['href'] == self_link['href'])


@vcr.use_cassette(str(cassettepath/'download_assets'))
def test_download_assets():
    t = NothingTask(get_test_payload(), workdir=testpath/'test-task-download-assets')
    t.download_assets(['metadata'])


if __name__ == "__main__":
    output = NothingTask.cli()
