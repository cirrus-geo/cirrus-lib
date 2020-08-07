import boto3
import json
import logging
import os

from boto3utils import s3
from typing import Dict, Optional, List

from pystac import STAC_IO, Catalog, CatalogType, Collection

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('CIRRUS_LOG_LEVEL', 'INFO'))


# envvars
DATA_BUCKET = os.getenv('CIRRUS_DATA_BUCKET', None)
PUBLIC_CATALOG = os.getenv('CIRRUS_PUBLIC_CATALOG', False)
STAC_VERSION = os.getenv('CIRRUS_STAC_VERSION', '1.0.0-beta.2')
DESCRIPTION = os.getenv('CIRRUS_STAC_DESCRIPTION', 'Cirrus STAC')
AWS_REGION = os.getenv('AWS_REGION')

ROOT_URL = f"s3://{DATA_BUCKET}/catalog.json"


def s3stac_read(uri):
    if uri.startswith('s3'):
        return json.dumps(s3().read_json(uri))
    else:
        return STAC_IO.default_read_text_method(uri)

def s3stac_write(uri, txt):
    extra = {
        'ContentType': 'application/json'
    }
    if uri.startswith('s3'):
        s3().upload_json(json.loads(txt), uri, extra=extra, public=PUBLIC_CATALOG)
    else:
        STAC_IO.default_write_text_method(uri, txt)

STAC_IO.read_text_method = s3stac_read
STAC_IO.write_text_method = s3stac_write


def get_root_catalog() -> Dict:
    """Get Cirrus root catalog from s3

    Returns:
        Dict: STAC root catalog
    """
    if s3().exists(ROOT_URL):
        cat = Catalog.from_file(ROOT_URL)
    else:
        catid = DATA_BUCKET.split('-data-')[0]
        cat = Catalog(id=catid, description=DESCRIPTION)
    logger.debug(f"Fetched {cat.describe()}")
    return cat


# add this collection to Cirrus catalog
def add_collection(collection):
    cat = get_root_catalog()
    col = Collection(**collection)
    cat.add_child(col)
    cat.normalize_and_save(ROOT_URL, CatalogType=CatalogType.ABSOLUTE_PUBLISHED)
    return cat