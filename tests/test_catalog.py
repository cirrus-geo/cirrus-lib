import os
import json
import unittest

from cirruslib import Catalog

testpath = os.path.dirname(__file__)


class TestClassMethods(unittest.TestCase):

    def open_fixture(self, filename='test-catalog.json'):
        with open(os.path.join(testpath, filename)) as f:
            data = json.loads(f.read())
        return data

    def test_open_catalog(self):
        data = self.open_fixture()
        cat = Catalog(**data)
        import pdb; pdb.set_trace()    
