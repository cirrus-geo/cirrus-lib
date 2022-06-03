# from cirrus.lib import statedb
import os
import sys
from unittest.result import failfast

from .src.cirrus.lib import statedb
from .src.cirrus.lib.process_payload import ProcessPayload

# sys.path.insert(
#     1,
#     "/Users/arthur/code/hfh-cirrus/docker/geospatial-single-task/external_libs/cirrus_lib/src/cirrus/lib",
# )


os.environ["CIRRUS_PAYLOAD_BUCKET"] = "app-cirrus-arthur-dev-payloads"
os.environ["CIRRUS_STATE_DB"] = "app-cirrus-arthur-dev-state"

state_db_arn = (
    "arn:aws:dynamodb:us-west-2:516414369703:table/app-cirrus-arthur-dev-state"
)

test_db = statedb.StateDB(table_name=os.environ["CIRRUS_STATE_DB"])


failed_runs = test_db.get_items_page(
    collections_workflow="fusion_Hydrosat",
    state="FAILED",
    limit=1000,
    error_code="Runtime.ImportModuleError",
)

for run in failed_runs["items"]:
    print(run["last_error"])
