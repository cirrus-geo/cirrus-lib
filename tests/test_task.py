#!/usr/bin/env python
import requests
from cirrus.lib.task import Task


class CopyTest(Task):

    _name = 'copy-test'
    _description = 'Copy assets from a STAC Item'
    _version = '0.1.0'

    def process(self):
        # download all assets locally
        self.download_assets()

        # upload all assets
        self.upload_assets()


if __name__ == "__main__":
    output = CopyTest.cli()