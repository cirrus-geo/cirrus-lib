# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]


## [v0.8.0] - 2022-09-12

### Added

* Ability to filter statedb job queries using a specific beginswith string ([#49])
* Add `start_datetime` and `end_datetime` message attributes for messages to
  `CIRRUS_PUBLISH_SNS` ([#53])

### Changed

* `ProcessPayload.process['output_options'] has been deprecated in favor of
  `'upload_options'` ([#51])

### Fixed

* Missing `datetime` will not fail message publishing ([#53])


## [v0.7.0] - 2022-02-17

### Added

* Support for an `ABORTED` workflow state ([#44])


## [v0.6.2] - 2022-02-07

### Fixed

* Issues when payloads are duplicated ([#41])

## [v0.6.1] - 2022-01-13

### Added

* support for item filtering when chainging workflows ([#37])
* official support for python 3.9 and 3.10 ([#38])


## [v0.6.0] - 2022-01-06

### ⚠️ Breaking changes

* Package now installed in `cirrus` namespace as `cirrus.lib`.
  Change all imports from `cirruslib` to `cirrus.lib`
* `Catalog` and `Catalogs` now `ProcessPayload` and `ProcessPayloads`
* `catalog.py` module renamed `process_payload.py`
* `ProcessPayload.process()` converted to `__call__()` method
* `ProcessPayload.process` is now a property that will return the
  current process definition in the case of a chained process array
* `ProcessPayload.from_payload()` renamed `from_event()`
* `ProcessPayload.publish_to_s3()` renamed `publish_items_to_s3()`
* `ProcessPayload.publish_to_sns()` renamed `publish_items_to_sns()`
  * Note that `ProcessPayload.publish_to_sns()` still exists but now
    publishes whole payload to an SNS topic, not each item
* All instances of the term `catalog` replaced by `payload`
* All instances of the abbreviation `cat` replaced by `payload`
* All instances of `catid` replaced by `payload_id`
* `ProcessPayload.get_catalog()` renamed `get_payload()`
* `ProcessPayloads.catids` renamed `payload_ids`
* `ProcessPayloads.from_catids()` renamed `from_payload_ids()`
* `StateBD.catid_to_key()` renamed `payload_id_to_key()`
* `StateBD.key_to_catid()` renamed `key_to_payload_id()`
* `StateBD.get_input_catalog_url()` renamed `get_input_payload_url()`
* env var `CIRRUS_CATALOG_BUCKET` renamed `CIRRUS_PAYLOAD_BUCKET`
* env var `BASE_WORKFLOW_ARN` renamed `CIRRUS_BASE_WORKFLOW_ARN`

### Added
* readme badges by @jkeifer ([#33]])
* support for workflow chaining by @jkeifer ([#32]])

### Changed
* move code under `/src` and change to `cirrus` namespace package
  by @jkeifer ([#31]])
* `Catalog` renamed to `ProcessPayload` and all references to `catalog`
  changed to `payload` by @jkeifer ([#34]])
* tests default to `us-west-2` region if not otherwise set in env vars
  by @jkeifer ([c919fad])

### Fixed
* exception traceback logging now shows stacktrace @jkeifer ([#30]])
* codecov reporting now works by @jkeifer ([#33]])
* `ProcessPayloads.process()` now returns list of processed `payload_id`s
  by @jkeifer ([02ff5e3])

### Removed
* version now tracked through git tags not `version.py` by @jkeifer ([#31]])


## [v0.5.1] - 2021-10-01

### Changed
- The `outputs` parameter to `stateddb.set_completed()` is no longer required, but now optional
- Added `statedb.set_outputs() to set outputs indepentent of execution state

## [v0.5.0] - 2021-08-20

### Removed
- `stac` module and global PySTAC dependency

## [v0.4.6] - 2021-07-15

### Changed
- Update boto3-utils minimum version
- `get_s3_session` now only handles error from missing secrets when trying to get bucket credentials

## [v0.4.5] - 2021-07-12

### Fixed
- Don't log S3 credentials

## [v0.4.4] - 2021-04-23

### Added
- Ability to query catalog for item(s) based on property values, use either `get_items_by_properties` or `get_item_by_properties` methods

## [v0.4.3] - 2021-03-30

### Fixed
- Race condition when setting processing of new catalogs
- Pagination of items in Cirrus API

## [v0.4.2] - 2021-01-12

### Added
- Add support for sorting of queries based on updated column

## [v0.4.1] - 2020-11-16

### Fixed
- Bug preventing rerun of inputs

## [v0.4.0] - 2020-11-13

### Added
- Expanded unit tests
- `status` attribute to published SNS attributes, either `created` or `updated`

### Changed
- DynamoDB state database schema changed:
    - `input_collections` -> `collections_workflow`, combines collections string and workflow name
    - `id` -> `itemids`, IDs of all input STAC Items, no longer prefaced with workflow (moved to `collections_workflow`)
    - `output_collections` field removed
    - `current_state` -> `state_updated`, same contents
    - `updated` field added containing just the updated datetime
    - `created_at` -> `created`
    - `output_urls` -> `outputs`, still a List of canonical STAC Item URLs
    - `error_message` -> `last_error`, contains the last execution error if input has ever failed
    - `execution` -> `executions`, now a list of all executions for this input catalog.
- Cirrus State Item changed:
    - `input_collections` -> `collections`
    - `created_at` -> `created`
    - `input_catalog` -> `catalog`
    - `output_urls` -> `outputs`
    - `error_message` -> `last_error`, now stores last execution error
    - `execution` -> `executions`, now a list of all executions for this input catalog
    - `output_collections` removed
    - `updated` added

## [v0.3.3] - 2020-10-27

### Fixed
- Catalog logging when updating

## [v0.3.2] - 2020-10-25

### Added
- `cirruslib.logging` module configuring structured (JSON) logging and get_task_logger for logging from tasks
- `cirruslib.stac` module for working with the Cirrus static STAC catalog on s3, uses PySTAC
- `utils.dict_merged` function for doing recursive merges

### Changed
- Parsing payload for a task should now use `Catalog.from_payload` instead of `Catalogs.from_payload`, which returns a `Catalog` instead of an array of `Catalog` objects that always had a length of one
- Claned up logging across all modules

### Removed
- `Catalogs.from_payload`, replaced by `Catalog.from_payload`
- `QUEUED` as potential processing state

## [v0.3.1] - 2020-09-27

### Changed
- output_options->collections is now optional, if not provided than item collections are not updated

### Fixed
- `process` function will reraise any error occuring while setting up processing so can be retried with redrivepolicy

## [v0.3.0] - 2020-09-02

### Changed
- Catalog.from_payload will get output payload from Batch via a separate file "<original-payload>\_out.json" rather than the original, as of Cirrus 0.2.0 Batch processes will write output to this new file rather than overwriting the input file

## [v0.2.1] - 2020-09-02

### Fixed
- Canonical link replaces previous canonical link

## [v0.2.0] - 2020-08-25

### Added
- Statedb.add_item function adds Item to DB with state=PROCESSING, must pass in Execution ARN
- Statedb.add_failed_item function adds Item to DB with state=FAILED, must pass in error message
- `process` Lambda added to consume from Cirrus Queue and start workflow (combines previous `validation` and `start-workflow` Lambdas)

### Changed
- Catalog no longer automatically adds fields as needed (e.g., a default process block, id), unless `update=True` is passed

## [v0.1.3] - 2020-08-13

### Changed
- Catalog() now sets the collection IDs of all Items based on the contents of `process->output_options->collections`. This ensures any uploaded items in any task have the correct collection

### Fixed
- Validate for collections in process['output_options'] rather than top level which always failed

## [v0.1.2] - 2020-08-10

### Changed
- No longer overwrite catalog ID if already provided

## [v0.1.1] - 2020-08-08

### Changed
- boto3-utils updated to 0.3.1

## [v0.1.0] - 2020-08-07

Initial Release

[Unreleased]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.8.0...main
[v0.8.0]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.7.0...v0.8.0
[v0.7.0]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.6.2...v0.7.0
[v0.6.2]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.6.1...v0.6.2
[v0.6.1]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.6.0...v0.6.1
[v0.6.0]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.5.1...v0.6.0
[v0.5.1]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.5.0...v0.5.1
[v0.5.0]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.4.6...v0.5.0
[v0.4.6]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.4.5...v0.4.6
[v0.4.5]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.4.4...v0.4.5
[v0.4.4]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.4.3...v0.4.4
[v0.4.3]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.4.2...v0.4.3
[v0.4.2]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.4.1...v0.4.2
[v0.4.1]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.4.0...v0.4.1
[v0.4.0]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.3.3...v0.4.0
[v0.3.3]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.3.2...v0.3.3
[v0.3.2]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.3.1...v0.3.2
[v0.3.1]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.3.0...v0.3.1
[v0.3.0]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.2.1...v0.3.0
[v0.2.1]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.2.0...v0.2.1
[v0.2.0]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.1.3...v0.2.0
[v0.1.3]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.1.2...v0.1.3
[v0.1.2]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.1.1...v0.1.2
[v0.1.1]: https://github.com/cirrus-geo/cirrus-lib/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/cirrus-geo/cirrus-lib.git@0.1.0

[#30]: https://github.com/cirrus-geo/cirrus-lib/pull/30
[#31]: https://github.com/cirrus-geo/cirrus-lib/pull/31
[#32]: https://github.com/cirrus-geo/cirrus-lib/pull/32
[#33]: https://github.com/cirrus-geo/cirrus-lib/pull/33
[#34]: https://github.com/cirrus-geo/cirrus-lib/pull/34
[#37]: https://github.com/cirrus-geo/cirrus-lib/pull/37
[#38]: https://github.com/cirrus-geo/cirrus-lib/pull/38
[#41]: https://github.com/cirrus-geo/cirrus-lib/pull/41
[#49]: https://github.com/cirrus-geo/cirrus-lib/pull/49

[#44]: https://github.com/cirrus-geo/cirrus-lib/issues/44
[#51]: https://github.com/cirrus-geo/cirrus-lib/issues/51
[#53]: https://github.com/cirrus-geo/cirrus-lib/issues/53

[c919fad]: https://github.com/cirrus-geo/cirrus-lib/commit/c919fadb83bb4f5cdfd082d482e25975ce12aa2c
[02ff5e3]: https://github.com/cirrus-geo/cirrus-lib/commit/02ff5e33412026b1fedda97727eef66715a27492
