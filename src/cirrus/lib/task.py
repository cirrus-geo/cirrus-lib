import argparse
import json
import logging
from os import makedirs
import os.path as op
from shutil import rmtree
import sys
from tempfile import mkdtemp
from typing import Dict, Optional

from boto3utils import s3

from cirrus.lib.logging import get_task_logger
from cirrus.lib.process_payload import ProcessPayload


class Task(object):

    _name = 'task'
    _description = 'A task for doing things'
    _version = '0.1.0'

    def __init__(self, event: Dict, local: Optional[str]=None):

        # parse event
        payload = ProcessPayload.from_event(event)

        # use PySTAC here
        self._items = payload['features']
        self._process = payload['process']
        self._output_items = []

        # set up logger
        self.logger = get_task_logger(self._name, payload=payload)

        # create temporary work directory if not running locally
        self._workpath = mkdtemp() if local is None else local
        makedirs(self._workpath, exist_ok=True)

        self.validate()


    @property
    def get_parameters(self):
        return self._process['tasks'].get(self._name, {})

    @property
    def get_output_options(self):
        return self._process.get('output_options', {})


    def validate():
        # put validation logic on input Items and process definition here
        pass


    def add_output_item(self):
        # create a derived output item
        for item in self._items:
            links = [l['href'] for l in item['links'] if l['rel'] == 'self']
            if len(links) == 1:
                # add derived from link
                item ['links'].append({
                    'title': 'Source STAC Item',
                    'rel': 'derived_from',
                    'href': links[0],
                    'type': 'application/json'
                })

    def run(self):
        """Main task logic

        Returns:
            [type]: [description]
        """

        # download assets of interest, this will update self._items
        self.download_assets(['key1', 'key2'])

        # do some stuff

        self.upload_assets(['key1', 'key2'])

    def output_payload() -> Dict:
        # assemble return payload
        return {

        }

    @classmethod
    def handler(*args, **kwargs) -> Task:
        try:
            task = Task(*args, **kwargs)
            task.run()
            return task.output_payload()
        except Exception as err:
            msg = f"**task** failed: {err}"
            task.logger.error(msg, exc_info=True)
            raise Exception(msg)
        finally:
            # remove work directory if not running locally
            if task.local is None:
                task.logger.debug('Removing work directory %s' % task._workpath)
                rmtree(task._workpath)
        
    @classmethod
    def parse_args(cls, args):
        """ Parse CLI arguments """
        dhf = argparse.ArgumentDefaultsHelpFormatter
        parser0 = argparse.ArgumentParser(description=cls._description)

        pparser = argparse.ArgumentParser(add_help=False)
        pparser.add_argument('--version', help='Print version and exit', action='version', version=cls._version)
        pparser.add_argument('--log', default=2, type=int,
                                help='0:all, 1:debug, 2:info, 3:warning, 4:error, 5:critical')
        subparsers = parser0.add_subparsers(dest='command')

        # process subcommand
        h = 'Locally process (development)'
        parser = subparsers.add_parser('local', parents=[pparser], help=h, formatter_class=dhf)
        parser.add_argument('filename', help='Full path of payload to process')
        parser.add_argument('--workdir', help='Use this as work directory', default='')

        # Cirrus process subcommand
        h = 'Process Cirrus STAC Process Catalog'
        parser = subparsers.add_parser('cirrus', parents=[pparser], help=h, formatter_class=dhf)
        parser.add_argument('url', help='s3 url to STAC Process Catalog')

        # turn Namespace into dictionary
        pargs = vars(parser0.parse_args(args))
        # only keep keys that are not None
        pargs = {k: v for k, v in pargs.items() if v is not None}

        if pargs.get('command', None) is None:
            parser.print_help()
            sys.exit(0)

        return pargs

    @classmethod
    def cli(cls):
        args = cls.parse_args(sys.argv[1:])
        cmd = args.pop('command')

        # logging
        logging.basicConfig(stream=sys.stdout,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=args.pop('log') * 10)
        # quiet these loud loggers
        quiet_loggers = ['botocore', 's3transfer', 'urllib3']
        for ql in quiet_loggers:
            logging.getLogger(ql).propagate = False

        if cmd == 'local':
            with open(args['filename']) as f:
                payload = json.loads(f.read())
            cls.handler(payload, local=args['workdir'])
        if cmd == 'cirrus':
            # fetch input catalog
            catalog = s3().read_json(args['url'])
            catalog = cls.handler(catalog)
            # upload return payload
            s3().upload_json(catalog, args["url"].replace('.json', '_out.json'))