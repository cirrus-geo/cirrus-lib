import os
import json
import unittest

from cirruslib import Catalog

testpath = os.path.dirname(__file__)


class TestClassMethods(unittest.TestCase):

    def open_fixture(self, filename='test-catalog.json'):
        with open(os.path.join(testpath, 'fixtures', filename)) as f:
            data = json.loads(f.read())
        return data

    def test_open_catalog(self):
        data = self.open_fixture()
        cat = Catalog(**data)
        assert(cat['id'] == "sentinel-s2-l2a/workflow-cog-archive/S2B_17HQD_20201103_0_L2A")

    def test_update_catalog(self):
        data = self.open_fixture()
        del data['id']
        del data['features'][0]['links']
        cat = Catalog(**data, update=True)
        assert(cat['id'] == "sentinel-s2-l2a/workflow-cog-archive/S2B_17HQD_20201103_0_L2A")

    def test_from_payload(self):
        data = self.open_fixture('sqs-payload.json')
        cat = Catalog.from_payload(data, update=True)
        assert(len(cat['features']) == 1)
        assert(cat['id'] == 'sentinel-s2-l2a-aws/workflow-publish-sentinel/tiles-17-H-QD-2020-11-3-0')

    def test_assign_collections(self):
        cat = Catalog(self.open_fixture())
        cat['process']['output_options']['collections'] = {'test': '.*'}
        cat.assign_collections()
        assert(cat['features'][0]['collection'] == 'test')

    def test_sns_attributes(self):
        cat = Catalog(self.open_fixture())
        attr = Catalog.sns_attributes(cat['features'][0])
        assert(attr['cloud_cover']['StringValue'] == '51.56')
        assert(attr['datetime']['StringValue'] == '2020-11-03T15:22:26Z')

    def test_get_features_by_properties(self):
        data = self.open_fixture()
        data['process']['item_queries'] = {'test': {'platform':'sentinel-2b'}}
        cat = Catalog.from_payload(data)
        assert(cat.get_features_by_properties("test") == data['features'])
