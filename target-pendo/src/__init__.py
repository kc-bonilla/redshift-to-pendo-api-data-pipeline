#!/usr/bin/env python3
import io
import sys
import json
import asyncio
import argparse
from math import ceil
from itertools import tee
from collections import Counter
from collections.abc import MutableMapping
import httpx
import backoff
from backoff import on_exception, expo
from ratelimit import limits, RateLimitException
from jsonschema.validators import Draft4Validator
from target_pendo.logger import SyncLogger
from target_pendo.exceptions import PendoClientResponseError, TargetPendoException, WriteError

LOGGER = SyncLogger(__name__).logger
MAX_ATTEMPTS = 5
FIVE_MINUTES = 300.0  # Necessary for passing 'period' arg to @limits decorator
R_MAX = 100000  # Default for max recursion depth to avoid sys error
NL = "\n"  # Newline constant for easier multiline logging

# should help resolve the WriteError/Errno 32:Broken Pipe
# failures from overloading Pendo Client
TIMEOUT = httpx.Timeout(
    connect=None,
    read=None,
    write=None,
    pool=None
)
LIMITS = httpx.Limits(
    max_keepalive_connections=2,
    max_connections=40,
    keepalive_expiry=FIVE_MINUTES
)


class Endpoints:
    """ * endpoint: API endpoint relative path, when added to the base URL, creates the full path
        * kind: type of record to be updated ('account' or 'visitor')
        * group: type of Pendo attribute to be updated ('agent' or 'custom')
        """

    path = '/api/v1/metadata/{}/{}/value'

    def __init__(self, stream):
        self.stream = stream
        self.base = 'https://app.pendo.io'
        self.kinds = ['account', 'visitor']
        self.group = 'custom'
        self.kind = self.get_kind()
        self.endpoint = self.build_endpoint()
        self.url = self.build_url()

    # create and store {kind} for stream's url path
    def get_kind(self):
        for kd in self.kinds:
            if kd in self.stream:
                self.kind = kd
        return self.kind

    # build stream's endpoint for requests to Pendo
    def build_endpoint(self):
        self.endpoint = Endpoints.path.format(self.kind, self.group)
        return self.endpoint

    # build stream's url for requests to Pendo
    def build_url(self):
        self.url = self.base + self.endpoint
        return self.url


class BatchArgs:

    def __init__(self, args=None, defaults=None) -> object:
        self.max_bytes = args.batch_bytes or defaults.bytes
        self.max_records = args.batch_records or defaults.records
        self.request_delay = args.request_delay or defaults.request_delay
        self.rate_limit = args.rate_limit or defaults.rate_limit
        self.max_attempts = args.attempts or defaults.attempts

    def to_dict(self):
        """Create batch_options dict from class object"""

        batch_options = {
            'bytes': self.max_bytes,
            'records': self.max_records,
            'request_delay': self.request_delay,
            'rate_limit': self.rate_limit,
            'retries': self.max_attempts
        }
        LOGGER.info(
            f"BATCH OPTIONS: {batch_options}"
        )
        return batch_options


class DefaultArgs:
    """Defaults for request/batch constraints"""

    def __init__(self):
        self.bytes = 5000000
        self.records = 500
        self.request_delay = 0.00
        self.rate_limit = 10
        self.attempts = 5


class Headers:
    """Place to store standard request headers"""

    def __init__(self, int_key):
        self.standard = {
            'User-Agent': 'Singer-ShootProof',
            'Accept-Encoding': 'gzip, deflate',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-Pendo-Integration-Key': int_key
        }


class ConfigArgs:
    def __init__(self, args):
        self.cfg_file = args.config
        self.config = self.load_config()
        self.int_key = self.config.get('integration_key')

    def load_config(self):
        with open(self.cfg_file) as cfg:
            self.config = json.load(cfg)
        return self.config


class StreamProps:
    """Creating class to assign common static variables
    to as attrs instead of declaring them as global across
    several functions or passing repeatedly
    """

    url = None
    int_key = None
    all_streams = None
    completed_streams = []

    def __init__(self):
        self.stream = None
        self.version = None
        self.primary_key = None
        self.field_mappings = None
        self.total_batches = None
        self.batches_built = 0
        self.batches_completed = 0
        self.total_records = None
        self.record_count = 0
        self.pending_requests = []
        self.failed_requests = []
        self.stream_totals = Counter()
        self.batch_times = [0.000]
        self.progress = None
        self.state = None

    def add_record(self):
        self.record_count += 1
        return self.record_count

    def add_pending(self, batch):
        self.pending_requests.append(batch)
        self.batches_built += 1
        return self.batches_built

    def drop_pending(self):
        self.batches_completed += 1
        self.pending_requests = self.pending_requests[self.batches_completed:]
        return self.pending_requests

    @classmethod
    def update_streams(cls, stream):
        if stream not in cls.completed_streams:
            cls.completed_streams.append(stream)
        cls.completed_streams.sort()
        cls.all_streams.sort()
        return cls.completed_streams

    def log_request_time(self, req_time):
        if req_time:
            self.batch_times.append(req_time)
            self.batch_times.sort(key=float)
        return self.batch_times

    def update_stream_totals(self, batch_result):
        if batch_result:
            self.stream_totals += batch_result
        return self.stream_totals

    def get_request_times(self):
        t1, t2 = tee(self.batch_times)
        next(t2, None)
        pairs = list(zip(t1, t2))
        time_deltas = [(round(pair[1] - pair[0], 3)) for pair in pairs]
        batch_keys = [f"BATCH{ct}" for ct in range(1, len(self.batch_times))]
        request_times = dict(zip(batch_keys, time_deltas))
        return request_times

    def get_sync_progress(self):
        """Returns completion % for stream sync"""

        if self.total_batches > 0:
            self.progress = round((self.batches_completed / self.total_batches) * 100, 2)
            LOGGER.info(
                f"{self.stream} SYNC: {self.progress}% COMPLETE"
            )
        return self.progress


async def finish_requests(session=None, stream_dict=None):
    stream = stream_dict.stream
    state = stream_dict.state
    emit_state(state)
    stream_totals = stream_dict.stream_totals
    failed = stream_dict.failed_requests
    num_fails = len(failed)
    failures = bool(num_fails > 0)
    if failures:
        retried = 0
        retrying = bool(retried <= num_fails)
        while retrying:
            batch = failed[retried]
            tasks = asyncio.ensure_future(post_request(session, batch, stream_dict))
            await asyncio.gather(*tasks, return_exceptions=True)
            retried += 1
            retrying = bool(retried <= num_fails)
    request_times = stream_dict.get_request_times()
    agg_time = round(sum(request_times.values()), 4)
    LOGGER.info(
        f"REQUEST TIMES: {request_times}{NL}"
        + f"{stream_totals['updated']} OF {stream_totals['total']} RECORDS SUCCEEDED{NL}"
        + f"TOTAL REQUEST TIME: {agg_time} SECONDS{NL}"
        + f"REQUESTS COMPLETE FOR {stream}"
    )
    # We add the stream to completed_streams
    # so we know what streams remain in sync
    StreamProps.update_streams(stream)
    LOGGER.debug(
        f"SYNC_DICT.STREAM: {stream}{NL}"
        + f"StreamProps.ALL_STREAMS: {StreamProps.all_streams}{NL}"
        + f"StreamProps.COMPLETED_STREAMS: {StreamProps.completed_streams}"
    )

    synced_all_streams = bool(StreamProps.completed_streams == StreamProps.all_streams)
    if synced_all_streams:
        LOGGER.info(
            f"ALL STREAMS COMPLETE,{NL}"
            "CLOSING CONNECTION WITH PENDO CLIENT"
        )
        sys.exit()
    else:
        return


def emit_state(state=None):
    """Write state to stdout"""

    if state:
        line = json.dumps(state)
        LOGGER.info(f"EMITTING STATE: {line}")
        sys.stdout.write(line)
        sys.stdout.flush()


def handle_failures(batch_response=None, batch=None, stream_dict=None):
    failed = stream_dict.failed_requests
    for error in batch_response.get('errors'):
        failed_id = error.get('id')
        failed_key = list(batch[0].keys())[0]
        failed_record = list(filter(lambda x: x[failed_key] == failed_id, batch))[0]
        failed.append(failed_record)
        # batch included in failures so the specific records
        # that failed in batch may be set aside for retries
    return failed


def exception_is_4xx(exc):
    return 400 <= exc.status < 500


def log_backoff(details):
    (_, exc, _) = sys.exc_info()
    LOGGER.info(
        "Error sending data to Pendo.\n" +
        f"Sleeping {details['wait']} seconds before trying again: {exc}"
    )

# TODO: replacement solution for ratelimit decorator,
# TODO: not working after switch to async Client
# Limit maximum number of concurrent tasks/request
# rate to not overload the server


@backoff.on_exception(
    backoff.expo,
    WriteError,
    max_tries=MAX_ATTEMPTS,
    giveup=exception_is_4xx,
    on_backoff=log_backoff
)
@limits(
    calls=1800,
    period=FIVE_MINUTES
)
async def post_request(session=None, batch=None, batch_idx=0, batch_lims=None, stream_dict=None):
    url = StreamProps.url = Endpoints(stream_dict.stream).url
    total_batches = stream_dict.total_batches
    request_delay = batch_lims.request_delay
    std_headers = Headers(StreamProps.int_key).standard
    LOGGER.debug(
        f"SENDING BATCH {batch_idx + 1} TO PENDO CLIENT @ {url}{NL}" +
        f"REQUEST BODY: {batch}"
    )
    try:
        response = await session.post(
            url=url, json=batch, headers=std_headers, timeout=TIMEOUT)
        await asyncio.sleep(request_delay)
        batch_result = response.json()
        status = response.status_code
        req_succeeded = bool(status // 100 == 2)  # floor div to check response status range
        failures = bool(batch_result.get('failed') > 0)
        # HTTP response status flow control
        if req_succeeded:
            stream_dict.log_request_time(response.elapsed.total_seconds())
            stream_dict.update_stream_totals(batch_result)
            stream_dict.drop_pending()
            batches_completed = stream_dict.batches_completed
            LOGGER.info(
                f"REQUEST {batches_completed} OF {total_batches} SUCCEEDED W/ STATUS {status}{NL}" +
                f"BATCH #{batches_completed} RESULTS: {batch_result}"
            )
            if batches_completed == total_batches:
                return await finish_requests(session, stream_dict)
        if failures:
            return handle_failures(batch_result, batch, stream_dict)
    except PendoClientResponseError as exc:
        msg = f"{exc.status}, {exc.response_body}"
        # PendoClientResponseError means we received > 2xx response
        raise TargetPendoException(
            f"ERROR PERSISTING DATA TO PENDO: {msg}"
        )  # Parse the msg from Client


async def concurrent_requests(task=None):
    semaphore = asyncio.Semaphore(10)
    async with semaphore:
        return await task


async def handle_requests(batch_lims=None, stream_dict=None):
    """Send built batches to Pendo Client, retry on exception"""

    pending = stream_dict.pending_requests
    async with httpx.AsyncClient() as session:
        batch_idxs = [pending.index(batch) for batch in pending]
        tasks = [
            asyncio.ensure_future(
                post_request(session, batch, batch_idx, batch_lims, stream_dict)
            ) for batch, batch_idx in zip(pending, batch_idxs)
        ]
        return await asyncio.gather(*(concurrent_requests(task) for task in tasks), return_exceptions=True)


def check_batch(batch=None, batch_lims=None, stream_dict=None):
    """Ensure updated values before checking against batch constraints"""

    max_bytes, max_records = batch_lims.max_bytes, batch_lims.max_records
    # gets size of flattened record (len(record) + dtype overhead memory) for r in batch
    total_records = stream_dict.total_records
    record_count = stream_dict.record_count
    batch_bytes = sum(sys.getsizeof(record) for record in batch)
    batch_records = len(batch)
    # checking on batch constraints/limits
    last_record = bool(record_count == total_records)
    enough_bytes = bool(batch_bytes >= max_bytes)
    enough_records = bool(batch_records >= max_records)
    batch_limiters = {
        'last_record': last_record,
        'byte_limit': enough_bytes,
        'record_limit': enough_records
    }
    LOGGER.debug(batch_limiters)
    if last_record:
        LOGGER.info(
            f"LAST RECORD: {last_record}{NL}" +
            "FINAL BATCH BUILD COMPLETE"
        )
    elif enough_bytes:
        LOGGER.info(
            f"BYTES LIMIT: {enough_bytes}"
        )
    elif enough_records:
        LOGGER.info(
            f"RECORDS LIMIT: {enough_records}"
        )
    else:
        pass  # continue looping through serialized stream lines
    return batch_limiters


def flatten(nested, parent_key='', sep='__'):
    items = []
    for key, val in nested.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(val, MutableMapping):
            items.extend(flatten(val, new_key, sep=sep).items())
        elif isinstance(val, list):
            items.append((new_key, str(val)))
        else:
            items.append((new_key, val))
    return dict(items)


def persist_records(incoming_stream=None, config=None, batch_lims=None):
    batch, schemas, validators = [], {}, {}
    max_records = batch_lims.max_records
    stream_dict = StreamProps()
    for line in incoming_stream:
        try:
            obj = json.loads(line)
            LOGGER.info(
                f"LINE: {obj}"
            )
        except json.decoder.JSONDecodeError:
            LOGGER.error(
                f"UNABLE TO PARSE: {line}"
            )
            raise
        try:
            msg_type = obj.get('type')
        except KeyError as err:
            LOGGER.error(
                f"THE MESSAGE {obj} IS MISSING REQUIRED KEY 'TYPE'"
            )
            LOGGER.error(err)
        if msg_type in ['counter', 'timer']:
            pass
        elif msg_type == 'ACTIVATE_VERSION':
            has_stream = bool(obj.get('stream'))
            has_version = bool(obj.get('version'))
            meets_all = all([
                has_stream,
                has_version
            ])
            if meets_all:
                current_stream = stream_dict.stream = obj.get('stream')
                current_version = stream_dict.version = obj.get('version')
                LOGGER.info(
                    f"CURRENT STREAM: {current_stream}{NL}" +
                    f"CURRENT VERSION: {current_version}"
                )
            elif not has_stream:
                raise Exception(
                    f"THE MESSAGE {obj} IS MISSING REQUIRED KEY 'STREAM'"
                )
            elif not has_version:
                raise Exception(
                    f"THE MESSAGE {obj} IS MISSING REQUIRED KEY 'VERSION'"
                )
        elif msg_type == 'VOLUME':
            has_count = bool(obj.get('count'))
            stream_match = bool(obj.get('stream') == current_stream)
            meets_all = all([
                has_count,
                stream_match
            ])
            if meets_all:
                total_records = stream_dict.total_records = obj.get('count')
                total_batches = stream_dict.total_batches = ceil(total_records / max_records)
                StreamProps.int_key = config.get('integration_key')
                LOGGER.info(
                    f"{total_records} TOTAL RECORDS{NL}" +
                    f"{total_batches} TOTAL BATCHES IN STREAM {current_stream}"
                )
        elif msg_type == 'RECORD':
            has_record = bool(obj.get('record'))
            has_stream = bool(obj.get('stream'))
            stream_match = bool(obj.get('stream') == current_stream)
            version_match = bool(obj.get('version') == current_version)
            schema_match = bool(obj.get('stream') in schemas)
            meets_all = all([
                has_record,
                has_stream,
                stream_match,
                version_match,
                schema_match
            ])
            if not meets_all:
                if not has_record:
                    raise Exception(
                        f"THE MESSAGE {obj} IS MISSING REQUIRED KEY 'RECORD'"
                    )
                if not has_stream:
                    raise Exception(
                        f"THE MESSAGE {obj} IS MISSING REQUIRED KEY 'STREAM'"
                    )
                if not stream_match:
                    raise Exception(
                        f"EXCEPTION FROM COMMINGLED STREAMS{NL}"
                        + f"Stream IN RECORD LINE: {obj.get('stream')}{NL}"
                        + f"CURRENT STREAM: {current_stream}"
                    )
                if not version_match:
                    raise Exception(
                        f"EXCEPTION FROM COMMINGLED Streams{NL}"
                        + f"VERSION IN RECORD LINE: {obj.get('version')}{NL}"
                        + f"CURRENT VERSION: {current_version}"
                    )
                if not schema_match:
                    raise Exception(
                        f"A RECORD FOR STREAM {obj.get('stream')}{NL}" +
                        "ENCOUNTERED BEFORE CORRESPONDING SCHEMA"
                    )
            elif meets_all:
                record_count = stream_dict.add_record()
                validators[obj['stream']].validate(obj['record'])
                LOGGER.info(
                    f"RECORD {record_count} OF {total_records}{NL}" +
                    f"for STREAM {current_stream}: VERSION {current_version}"
                )
                # TODO: write in-line documentation for this section and clean-up
                # TODO: create separate function for key mappings/swap
                record = flatten(obj.get('record'))
                if field_mappings:
                    # check for matching attribute names
                    for targ_attr, tap_attr in field_mappings.items():
                        fields_match = bool(targ_attr == tap_attr)
                        # if tap-target attr names are identical,
                        # modify attr name to prevent both from
                        # being deleted after passing val
                        if fields_match:
                            tap_attr_tmp = tap_attr + '_tmp'
                            record[tap_attr_tmp] = record[tap_attr]
                            tap_attr = tap_attr_tmp
                        record[targ_attr] = record[tap_attr]
                        # checking for correct dtype mapping
                        targ_attr_string = isinstance(record[targ_attr], str)
                        tap_attr_int = isinstance(record[tap_attr], int)
                        if targ_attr_string and tap_attr_int:
                            record[targ_attr] = str(record[tap_attr])
                        del record[tap_attr]
                # preparing request body for Client-defined structure
                # Pendo Bulk POST Request Structure:
                # {pkey: 'key', 'values': {targ_attr_1: 'val1', targ_attr_2: 'val2'...}}
                val_keys = set(record.keys()) - set(primary_key)
                # creates the set of values for that account/user
                values = {key: record[key] for key in val_keys}
                record['values'] = values
                for key in val_keys:
                    del record[key]
                batch.append(record)
                batch_bytes = sum(sys.getsizeof(record) for record in batch)
                batch_records = len(batch)
                # check current batch against batch constraints w/ each append
                batch_status = check_batch(
                    batch, batch_lims, stream_dict
                )
                limiters = list(batch_status.values())
                batch_done = any(limiters)
                if batch_done:
                    batches_built = stream_dict.add_pending(batch)
                    batch = []  # we clear batch again after append to pending
                    LOGGER.info(
                        f"BATCH BUILD {batches_built} OF {total_batches} COMPLETE{NL}" +
                        f"BYTES: {batch_bytes}, RECORDS: {batch_records}"
                    )
                    done_batching = batch_status.get('last_record')
                    if done_batching:
                        asyncio.run(handle_requests(batch_lims, stream_dict))
                        asyncio.run(finish_requests(session=None, stream_dict=stream_dict))
                        stream_dict = StreamProps()
                    else:
                        LOGGER.info(
                            f"BUILDING BATCH {batches_built + 1}"
                        )
            else:
                LOGGER.critical("UNSUPPORTED STREAM")
            state = None
        elif msg_type == 'STATE':
            has_value = bool(obj.get('value'))
            if has_value:
                state = stream_dict.state = obj.get('value')
                emit_state(state)
                LOGGER.info(
                    f"SETTING STATE TO {state}"
                )
        elif msg_type == 'SCHEMA':
            has_stream = bool(obj.get('stream'))
            has_schema = bool(obj.get('schema'))
            meets_all = all([
                has_stream,
                has_schema
            ])
            if meets_all:
                current_stream = stream_dict.stream = obj.get('stream')
                current_schema = schemas[current_stream] = obj.get('schema')
                primary_key = stream_dict.primary_key = [config.get(current_stream).get('primary_key')]
                field_mappings = stream_dict.field_mappings = config.get(current_stream).get('field_mappings')
                LOGGER.info(
                    f"CURRENT SCHEMA: {current_schema}"
                )
            elif not has_stream:
                raise Exception(
                    f"THE MESSAGE {obj} IS MISSING REQUIRED KEY 'STREAM'"
                )
            elif not has_schema:
                raise Exception(
                    f"THE MESSAGE {obj} IS MISSING REQUIRED KEY 'SCHEMA'"
                )
            validators[current_stream] = Draft4Validator(current_schema)
        else:
            raise Exception(
                f"UNKNOWN MESSAGE TYPE {obj.get('type')} IN MESSAGE {obj}"
            )
    return state


def handle_args():
    """Parses args/batch constraints"""

    LOGGER.info("PARSING TARGET-PENDO ARGS")
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Target Config File')
    parser.add_argument('--batch_bytes', type=int, help='Constraint: max # of bytes per batch')
    parser.add_argument('--batch_records', type=int, help='Constraint: max # of records per batch')
    parser.add_argument('--request_delay', type=float, help='Time(sec,float) to sleep btw requests')
    parser.add_argument('--rate_limit', type=int, help='Constraint: max # of requests per second')
    parser.add_argument('--attempts', type=int, help='Constraint: max # of requests upon failure')
    parser.add_argument('-v', '--verbose', help='Produce debug-level logging', action='store_true')
    parser.add_argument('-q', '--quiet', help='Suppress warning-level logging', action='store_true')
    args = parser.parse_args()
    LOGGER.info(f"ARGS: {vars(args)}")
    if args.verbose:
        LOGGER.setLevel('DEBUG')
    elif args.quiet:
        LOGGER.setLevel('WARNING')
    else:
        LOGGER.setLevel('INFO')
    if args.config:
        config_args = ConfigArgs(args).config
        LOGGER.info(
            f"TARGET-PENDO CONFIG OPTIONS: {config_args}"
        )
    defaults = DefaultArgs()
    batch_lims = BatchArgs(args, defaults)
    return {'config': config_args, 'batch_lims': batch_lims}


def check_recursion(r_max=R_MAX):
    """Check recursion depth due to recursion in requests loop"""

    r_lim = sys.getrecursionlimit()
    increase_lim = bool(r_lim < r_max)
    if increase_lim:
        r_lim = r_max
        sys.setrecursionlimit(r_lim)
        LOGGER.debug(
            f"MAX RECURSION DEPTH INCREASED TO {r_lim}"
        )
    return r_lim


def main_impl():
    check_recursion()
    config = handle_args().get('config')
    batch_lims = handle_args().get('batch_lims')
    # Listing all streams in target_config.json streams
    StreamProps.all_streams = list(set(list(config.keys())) - set({'integration_key'}))
    incoming_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    persist_records(incoming_stream, config, batch_lims)


def main():
    try:
        main_impl()
        # log CRITICAL line for exceptions caught at top level(root cause)
        # suppress stack trace if defined TargetPendoException
    except TargetPendoException as exc:
        for line in str(exc).splitlines():
            LOGGER.critical(line)
        sys.exit()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    """Main entry point"""
    main()
