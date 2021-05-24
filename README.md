# data-pipeline-development

## Redshift-Pendo Integration

Outbound integration to extract data attributes from our Data Warehouse and pipe it to Pendo Target to be consumed and joined on Pendo Accounts/Visitors data, allowing for better insights & user experiences.

### Singer Basics

**Tap**: Application that takes a configuration file and an optional state file as input and produces an ordered stream of record, state, & schema messages as output.

- **Record Message**: JSON-encoded data of any kind.
- **State Message**: Used to persist information between invocations of a Tap.
- **Schema Message**: Describes the datatypes of the records in the stream.

Pendo Target Request Parameters

Data Stores	
Visitors: General information on all 
visitors Pendo has ever seen.
Accounts: General information on all accounts Pendo has ever seen.
Events: Each interaction with your application by a visitor (click, page 
load, metadata, guide events)

Method
POST

URI
/api/v1/metadata/{kind}/{group}/value

Parameters
kind - "visitor" or "account"
group - type of metadata field: agent or custom

Data
JSON array (see example format)	Request Headers
content-type: application/json
x-pendo-integration-key: <PENDO_INTEGRATION_KEY>


Status Codes
200: The bulk update completed.
400: The format is unacceptable due to malformed JSON or missing field mappings.
408: The call took too long and timed out.


Attributes
Total number = Sum of updated + failed.
Failed number	 = Sum of length(missing) + length(errors).


Rate Limits & Service Protection API Limits
Pendo API allows for any number of records to be submitted for update, but the call is limited to five (5) minutes. Any calls that take longer will violate this service protection API Limit and result in a 408 Request Timeout.

ShootProof User ID  Platform User ID  Pendo’s Visitor ID

ShootProof Key			Pendo Key
Studio ID (shootproof.studio.id)		
Account ID
User ID (shootproof.user.id)			Visitor ID

Tap tap-redshift

description
Tap that extracts data from Redshift data warehouse and writes it to a standard stream in a JSON-based format, which can be piped to-and consumed by-any target.

arguments

		configuration file
o	description: contains required credentials and database connection properties for the tap.
o	filetype: JSON
o	arg options: (-c, --config)

catalog file
o	description: contains a list of stream objects that correspond to each available table in the Redshift schema designated in your config file.
o	filetype: JSON
o	arg options: (--catalog)


dependencies
Connection to Redshift
Python 3.6+.


discovery mode
~/Github/data-and-analytics/singer/tap-redshift/tap-redshift/bin/tap-redshift --config tap_rs_config.json -d

Target target-pendo

description
Takes sys.stdout stream piped from tap as sys.stdin stream…config file with ___

venv location
/Users/kbonilla/.virtualenvs/target-pendo/src/target-pendo/target_pendo

compilation folder structure
target-pendo/
├── target_pendo/     <-- Python package with source code
    └── target_pendo.py
    └── client.py
    └── request.py
    └── errors.py
    └── target_pendo.py
└── setup.py

config file location & content
target_rs_config.json
host
port
dbname	user password
start_date* 
schema**



*format: (yyyy-mm-ddthh:mm:ssz)
**optional

dependencies
- Pendo Admin access credentials may create custom fields via Data Mappings page in Pendo.




tap-redshift MVP target-pendo


1.	Updates Pendo Visitor/Accounts Custom Fields with any source attribute(s)
2.	Performs Full-Replication
3.	Performs Incremental Data Loads
4.	Maintains State Between Runs
5.	Versions States and Can Revert to Prior State

Each state saved to S3 bucket in event of need
For reversion to prior state

6.	Catches Tap Schema Changes
7.	Allows for Monitored Job Runs/Job Results
8.	Notifies on Failure (later)
Triggers SNS Topic Alert to DAE team Topic in AWS upon failure to prompt a response and troubleshooting
9.	Adjusts for Runtime Limits
I want to be able to Rate Limit API calls/sessions so that API Service ___ are not violated leading to diminished service and thus data health, and so I can complete the large jobs required for initial full replication by staying within rate limit bounds and moving a stream of s-sized chunks across n-batches until the job is complete

Factor of Safety * API Rate Limit * API Time Limit
No Matching Key in Pendo?
Pendo will only push data into matching objects. It will not create new records in Pendo. If no matching record is found the data will not push to Pendo.

Add Custom Fields to Be Pushed to Pendo**
Data types must match for custom field mappings the account/visitor object
 
tap-redshift REQUIREMENTS target-pendo




1)	Singer Redshift Tap: Extracts data from Redshift data warehouse and writes it to a standard stream in a JSON-based format, which can be piped to-and consumed by-any target.

As a ShootProof Pendo Data Consumer
	I want an integration from our internal Data Warehouse to Pendo
So that we can tailor a more holistic, customized experience for our users

AC

2)	Singer Pendo Target: Consumes JSON-based standard stream data from the Redshift Tap (or any other tap) and communicates with the Pendo API to set or update Pendo attributes in accordance with this incoming data.

As a ShootProof Pendo Data Consumer
	I want an integration from our internal Data Warehouse to Pendo
So that we can tailor a more holistic, customized experience for our users


AC
1.	Any attribute that lives inside a Redshift source table may be pushed into the Pendo Account or Visitor objects with a 1:1 primary key match and pre-existing Custom Field Mapping in Pendo


3)	Supports Scheduled Runs: Runs on a scheduled basis, detecting which records have been added, deleted, or changed, and inserts, updates, or deletes the corresponding data in the corresponding Pendo tables.

As a ShootProof Pendo Data Consumer
	I want _____________________________
So that ____________________________________________________________________

AC
2.	Maintains state by __________ between invocations to support incremental extraction.

4)	Supports Full Replication

As a ShootProof Pendo Data Consumer
	I want the ability to be able to perform a Full Replication from source data,
So that incremental loads will thereafter be feasible

AC
1.	Passes the state into a state.json file.
2.	Extracts all data from the source table each time the tap is invoked without a state file.

5)	Supports Incremental Replication

As a ShootProof Pendo Data Consumer
	I want the ability to incrementally update custom attributes in Pendo
So that synchronization time and risk exposure are minimized with each run.

AC
1.	Maintains state by __________ between invocations to support incremental extraction. 
2.	Passes the state into a state.json file.
3.		Replication_method and replication_key JSON attributes passed to respective stream’s metadata in the catalog.json file after run.

6)	Detects Schema Changes in Source Data, Updates Stream Catalog Accordingly
Taps detect schema changes in source databases and target connectors alter the destination tables automatically. Based on the schema change type:

7)	Not at risk of record corruption, Mistake Tolerant
needs to be idempotent.
This means if you PUT the same payload a second time the system state should not be changed.
If the intended payload is a mix of new and existing records, 'upserts' via POST call.


EXAMPLE: INITIAL FULL REPLICATION

sudo ~/Github/data-and-analytics/singer/tap-redshift/tap-redshift/bin/tap-redshift --config tap_rs_config.json --catalog catalog_full_rep.json | /Users/kbonilla/.virtualenvs/target-pendo/src/target-pendo/target_pendo --config target_config.json > state.json


The tap can be invoked in discovery mode to get the available tables and columns in the database
$ tap-redshift --config config.json -d
A full catalog tap is written to stdout, with a JSON-schema description of each table. 
Each source table directly corresponds to a Singer stream.

Redirect output from tap's discovery mode to a file to be modified when the tap is next invoked in sync mode.
To run tap in discovery mode and copy output into a catalog.json file:
$ tap-redshift -c config.json -d > catalog.json

Step 3: Select the tables you want to sync
In sync mode, tap-redshift requires a catalog file to be supplied, where the user must have selected which streams (tables) should be transferred. Streams are not selected by default.

For each stream in the catalog, find the metadata section. That is the section you will modify to select the stream and, optionally, individual properties too.

The stream itself is represented by an empty “breadcrumb” object.
You can select it by adding "selected": true to its metadata.

The tap can then be invoked in sync mode with the properties catalog argument:
tap-redshift -c config.json --catalog catalog.json | target-pendo -c config-dw.json

Step 4: Sync your data
FULL_TABLE replication is used by default.

Example:
"metadata": [
    {
        "breadcrumb": [],
        "metadata": {
            "selected": true,
            "selected-by-default": false,
            "replication-method": "INCREMENTAL",
            "replication-key": "updated_at",
            ...
        }
    }
]

Can now Invoke the tap again in sync mode. This time the output will have STATE message that contains a replication_key_value and bookmark for data that was extracted.

Redirect the output to a state.json file. Normally, the target will echo the last STATE after it has finished processing data.

Example
tap-redshift -c config.json --catalog catalog.json | \
    target-pendo -c config-dw.json > state.json
The state.json file should look like;
{
    "currently_syncing": null,
    "bookmarks": {
        "sample-dbname.public.sample-name": {
            "replication_key": "updated_at",
            "version": 1516304171710,
            "replication_key_value": "2013-10-29T09:38:41.341Z"
        }
    }
}

For subsequent runs, can invoke the incremental replication by passing the latest state in order to limit data only to what has been modified since the last execution.

EXAMPLE: INCREMENTAL REPLICATION
tail -1 state.json > latest-state.json; \
tap-redshift \
    -c config-redshift.json \
    --catalog catalog.json \
        -s latest-state.json | \
            target-pendo -c config.json > state.json

# Catalog discovery
discover:
    tap-redshift \
        -c config-redshift.json -d > catalog.json

sync:
    tail -1 state.json > latest-state.json; \
    tap-redshift \
      -c config-redshift.json \
      --catalog catalog.json \
      -s latest-state.json | \
        target-pendo -c config-dw.json > state.json



Retry Operations
Service protection API limit errors will return a Retry-After Duration value indicating the duration before any new requests from the user can be processed.

Interactive application retries
If the client is an interactive application, you should display a message that the server is busy while you re-try the request the user made. You may want to provide an option for the user to cancel the operation. Don't allow users to submit more requests until the previous request you sent has completed.


INFO METRIC: 
{"type": "counter", 
"metric": "record_count", "value": 331207, 
"tags": {"database": null, "table": "public.pendo_integration_account"}}

{"type": "ACTIVATE_VERSION", 
"stream": "pendo_integration_account", "version": 1614185051899}

{"type": "STATE", 
"value": {"currently_syncing": "dev.public.pendo_integration_account", "bookmarks": {"dev.public.pendo_integration_account": {"version": null}}}}

INFO METRIC:
{"type": "timer", 
"metric": "job_duration", "value": 27.544612169265747, 
"tags": {"job_type": "sync_table", "database": null, "table": "public.pendo_integration_account", "status": "succeeded"}}

{"type": "STATE", 
"value": {"currently_syncing": null, 
"bookmarks": {"dev.public.pendo_integration_account": {"version": null}}}}

INFO Completed sync

My application to create CLI code
Resolved circular module imports issue  GitHub
Ready for Full then incremental

Load testing
Add Attributes with aggregate functions



Pylint
target_pendo/__init__.py:
•	disable=W0105,W1201,W1203,W0511,W0640,R0903,R0912,R0915,R0914,R1702,C0103



Changelog


An idiosyncratic feature of Foreground’s Tap-Redshift-Target-Pendo Singer Integration is a POST request (althgouh resembling a GET request) made on the Tap-Redshift side (in sync.py) that utilizes the Pendo Aggregation API that allows us to query all Foreground Pendo Accounts/Visitors for those having UUID-formatted IDs. This indicates that the Account/Visitor has been active since a shift toward UUIDs was enacted in the Pendo Snippet and drastically reduces the Accounts/Visitor IDs to be queried from Redshift before updating that IDs associated attributes in Pendo.
 



EXAMPLE: INITIAL FULL REPLICATION

sudo ~/Github/data-and-analytics/singer/tap-redshift/tap-redshift/bin/tap-redshift --config tap_rs_config.json --catalog catalog.json | /Users/kbonilla/.virtualenvs/target-pendo/src/target-pendo/target_pendo --config target_config.json > state.json

Redirect output from tap's discovery mode to a file to be modified when the tap is next invoked in sync mode.
To run tap in discovery mode and copy output into a catalog.json file:
$ tap-redshift -c config.json -d > catalog.json

Select the tables you want to sync

tap-redshift -c tap_config.json --catalog catalog.json | target-pendo -c target_config.json > state.json

Sync your data

Can now Invoke the tap again in sync mode. This time the output will have STATE message that contains a replication_key_value and bookmark for data that was extracted.

Redirect the output to a state.json file. Normally, the target will echo the last STATE after it has finished processing data.

Example
The state.json file should look like;
{
    "currently_syncing": null,
    "bookmarks": {
        "sample-dbname.public.sample-name": {
            "replication_key": "updated_at",
            "version": 1516304171710,
            "replication_key_value": "2013-10-29T09:38:41.341Z"
        }
    }
}

For subsequent runs, can invoke the incremental replication by passing the latest state in order to limit data only to what has been modified since the last execution.

tail -1 state.json > latest-state.json;
tap-redshift -c tap_config.json --catalog catalog.json -s latest-state.json | target-pendo -c target_config.json > state.json


1.	open terminal
2.	ssh dae > passphrase
3.	open terminal
4.	cd Github/data-and-analytics/singer/tap-redshift/tap-redshift
5.	sudo ~/Github/data-and-analytics/singer/tap-redshift/tap-redshift/bin/tap-redshift -c tap_config.json --catalog catalog.json

sudo ~/Github/data-and-analytics/singer/tap-redshift/tap-redshift/bin/tap-redshift -c tap_config.json --catalog catalog.json | ~/.virtualenvs/target-pendo/bin/target-pendo -c target_config.json

sudo ~/Github/data-and-analytics/singer/tap-redshift/tap-redshift/bin/tap-redshift -c tap_config.json --catalog catalog3.json | ~/.virtualenvs/target-pendo/bin/target-pendo -c target_config.json


Pendo:
Working on the Zendesk target alongside the Pendo stuff

rate-limits, but not a prob unless concurrency, multithread pooling for
Pendo ID update
