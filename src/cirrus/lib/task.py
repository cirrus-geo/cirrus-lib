from abc import ABC, abstractmethod #abstractclassmethod
import argparse
from collections import OrderedDict
from concurrent.futures import process 
from copy import deepcopy
import json
import logging
from operator import getitem 
from os import makedirs
from pathlib import Path
from shutil import rmtree
import sys
from tempfile import mkdtemp
from typing import Dict, List, Optional, Union

from boto3utils import s3

from cirrus.lib.logging import get_task_logger
from cirrus.lib.process_payload import ProcessPayload
from cirrus.lib.transfer import download_item_assets, upload_item_assets

# types
PathLike = Union[str, Path]


class Task(ABC):

    _name = 'task'
    _description = 'A task for doing things'
    _version = '0.1.0'

    def __init__(self: "Task", payload: Dict,
                 local: Optional[bool]=False,
                 workdir: Optional[PathLike]=None,
                 skip_validation: Optional[bool] = False):
        # parse event
        payload = ProcessPayload.from_event(payload)

        self._payload = payload

        # The original items from the payload
        self.original_items = deepcopy(payload['features'])

        self.items = self._payload['features']

        # set up logger
        self.logger = get_task_logger(f"task.{self._name}", payload=payload)

        if not skip_validation:
            self.validate()

        # local mode? 
        self._local = local

        # create temporary work directory if workdir is None
        self._workdir = workdir
        if workdir is None:
            self._workdir = Path(mkdtemp())
            self._tmpworkdir = True
        else:
            self._workdir = Path(workdir)
            self._tmpworkdir = False
            makedirs(self._workdir, exist_ok=True)

    def __del__(self):
        # remove work directory if not running locally
        if self._tmpworkdir:
            self.logger.debug(f"Removing work directory {self._workdir}")
            rmtree(self._workdir)

    @property
    def id(self) -> str:
        return self._payload['id']

    @property
    def output_payload(self):
        processing_ext = 'https://stac-extensions.github.io/processing/v1.1.0/schema.json'
        for i in self.items:
            i['stac_extensions'].append(processing_ext)
            i['stac_extensions'] = list(set(i['stac_extensions']))
            i['properties']['processing:software'] = {
                self._name: self._version
            }
        self._payload['features'] = self.items
        # add provenance metadata

        return self._payload

    @property
    def process_definition(self) -> Dict:
        return self._payload['process']

    @property
    def parameters(self) -> Dict:
        return self.process_definition['tasks'].get(self._name, {})

    @property
    def output_options(self) -> Dict:
        return self.process_definition.get('output_options', {})

    def validate(self) -> bool:
        # put validation logic on input Items and process definition here
        return True

    def download_assets(self, assets: Optional[List[str]]=None):
        """Download provided asset keys for all items in payload. Assets are saved in workdir in a
           directory named by the Item ID

        Args:
            assets (Optional[List[str]], optional): List of asset keys to download. Defaults to all assets.
        """
        for i, item in enumerate(self.items):
            outdir = self._workdir / Path(item['id'])
            makedirs(outdir, exist_ok=True)
            self.items[i] = download_item_assets(item, path=outdir, assets=assets)

    def upload_assets(self, assets: Optional[List[str]]=None):
        if self._local:
            self.logger.warn('Running in local mode, assets not uploaded')
            return
        for i, item in enumerate(self.items):
            self.items[i] = upload_item_assets(item, **self.output_options)

    # this should be in PySTAC
    @classmethod
    def create_item_from_item(self, item):
        # create a derived output item
        links = [l['href'] for l in item['links'] if l['rel'] == 'self']
        if len(links) == 1:
            # add derived from link
            item ['links'].append({
                'title': 'Source STAC Item',
                'rel': 'derived_from',
                'href': links[0],
                'type': 'application/json'
            })
        return item

    @abstractmethod
    def process(self, **kwargs) -> List[Dict]:
        """Main task logic - virtual

        Returns:
            [type]: [description]
        """
        # download assets of interest, this will update self.items
        #self.download_assets(['key1', 'key2'])
        # do some stuff
        #self.upload_assets(['key1', 'key2'])
        return self.items

    @classmethod
    def handler(cls, payload, **kwargs):
        task = cls(payload, **kwargs)
        try:
            task.items = task.process(**task.parameters)
            return task.output_payload
        except Exception as err:
            task.logger.error(err, exc_info=True)
            raise err

    @classmethod
    def get_cli_parser(cls):
        """ Parse CLI arguments """
        dhf = argparse.ArgumentDefaultsHelpFormatter
        parser0 = argparse.ArgumentParser(description=cls._description)
        parser0.add_argument('--version', help='Print version and exit', action='version', version=cls._version)

        pparser = argparse.ArgumentParser(add_help=False)
        pparser.add_argument('--logging', default='INFO', help='DEBUG, INFO, WARN, ERROR, CRITICAL')
        subparsers = parser0.add_subparsers(dest='command')

        # process subcommand
        h = 'Locally process (development)'
        parser = subparsers.add_parser('local', parents=[pparser], help=h, formatter_class=dhf)
        parser.add_argument('filename', help='Full path of payload to process')
        h = 'Use this as work directory. Will be created but not deleted)'
        parser.add_argument('--workdir', help=h, default=None, type=Path)
        parser.add_argument('--save', help='Save output with provided filename', default=None)

        # Cirrus process subcommand
        h = 'Process Cirrus STAC Process Catalog'
        parser = subparsers.add_parser('cirrus', parents=[pparser], help=h, formatter_class=dhf)
        parser.add_argument('url', help='url (s3 or local) to Cirrus Process Payload')
        return parser0

    @classmethod
    def parse_args(cls, args, parser=None):
        if parser is None:
            parser = cls.get_cli_parser()
        # turn Namespace into dictionary
        pargs = vars(parser.parse_args(args))
        # only keep keys that are not None
        pargs = {k: v for k, v in pargs.items() if v is not None}

        if pargs.get('command', None) is None:
            parser.print_help()
            sys.exit(0)

        return pargs

    @classmethod
    def cli(cls, parser=None):
        args = cls.parse_args(sys.argv[1:], parser=parser)
        cmd = args.pop('command')

        # logging
        loglevel = args.pop('logging')
        logging.basicConfig(stream=sys.stdout,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=loglevel)
        # quiet these loud loggers
        quiet_loggers = ['botocore', 's3transfer', 'urllib3']
        for ql in quiet_loggers:
            logging.getLogger(ql).propagate = False

        if cmd == 'local':
            save = args.pop('save', None)
            # open local payload
            with open(args.pop('filename')) as f:
                payload = json.loads(f.read())
            # run task handler
            output = cls.handler(payload, local=True, **args)
            # save task output
            if save:
                with open(save, 'w') as f:
                    f.write(json.dumps(output))
        if cmd == 'cirrus':
            # get remote payload
            payload = s3().read_json(args['url'])
            # run task handler
            output = cls.handler(payload)
            # upload task output
            s3().upload_json(output, args["url"].replace('.json', '_out.json'))
