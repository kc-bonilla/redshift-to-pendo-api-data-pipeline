import sys
import copy
import time
import datetime
import pendulum
import simplejson as json
import httpx
import asyncio
from validators import uuid
from tap_redshift import bookmarks, messages, parsed_args
from tap_redshift.streams import STREAMS
from singer import logger, metadata, metrics, utils

LOGGER = logger.get_logger()
NL = "\n"  # adding newline constant for easier multiline logging
QUERY_LIMIT = parsed_args.query_limit
START_DATE = parsed_args.start_date
INT_KEY = parsed_args.target_int_key
TIMEOUT = httpx.Timeout(connect=None, read=None, write=None, pool=None)
LIMITS = httpx.Limits(max_keepalive_connections=1, max_connections=5, keepalive_expiry=300.0)
HEADERS = {
    'User-Agent': 'Singer-ShootProof',
    'Accept-Encoding': 'gzip, deflate',
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'X-Pendo-Integration-Key': INT_KEY
}


async def fetch_uuids(stream=None):
    """Function to make GET request to Pendo API & filter for users with UUIDs,
    meaning they have had activity since updating Pendo PKEY
    """
    async with httpx.AsyncClient(timeout=TIMEOUT, limits=LIMITS) as session:
        stream_target_entity = STREAMS[stream]['target_entity']
        stream_target_pkey = STREAMS[stream]['primary_key']
        pendo_uuids = []
        aggr_url = 'https://app.pendo.io/api/v1/aggregation'  # Pendo Aggregation API endpoint
        # Building query for Aggregation Endpoint
        data = "{\"response\":{\"mimeType\":\"application/json\"},"
        data += "\"request\":{\"pipeline\":[{\"source\":{\"%s\":null}}," % stream_target_entity
        data += "{\"filter\":\"len(%s) == 36\"}," % stream_target_pkey
        data += "{\"select\": {\"%s\":\"%s\"}}]}}" % (stream_target_pkey, stream_target_pkey)
        LOGGER.info(
            f"FETCHING UUIDs FROM PENDO FOR:{NL}"
            + f"PENDO ENTITY: {stream_target_entity}{NL}"
            + f"BY PENDO KEY: {stream_target_pkey}"
        )
        response = await session.post(url=aggr_url, data=data, headers=HEADERS)
        results = list(response.json()['results'])
        for record in results:
            user_id = record.get(stream_target_pkey)
            pendo_uuids.append(user_id) if uuid(user_id) else ...
    return pendo_uuids


def do_sync(conn=None, db_schema=None, catalog=None, state=None):
    """Writes all Singer messages to stdout and flushes"""
    LOGGER.info("STARTING REDSHIFT SYNC")
    for message in messages.generate_messages(conn, db_schema, catalog, state):
        if message is not None:
            sys.stdout.write(
                json.dumps(
                    message.asdict(),
                    default=coerce_datetime,
                    use_decimal=True
                ) + NL
            )
            sys.stdout.flush()
        else:
            pass
    LOGGER.info("COMPLETED SYNC")


def coerce_datetime(dt_time=None):
    if isinstance(dt_time, (datetime.datetime, datetime.date)):
        return dt_time.isoformat()
    raise TypeError(
        f"TYPE {type(dt_time)} IS NOT SERIALIZABLE"
    )


def sync_table(connection=None, catalog_entry=None, state=None):
    columns = list(catalog_entry.schema.properties.keys())
    formatted_start_date = None
    if not columns:
        LOGGER.warning(
            f"NO COLUMNS SELECTED FOR TABLE {catalog_entry.table},{NL}" +
            "SKIPPING IT"
        )
        return
    stream, tap_stream_id = catalog_entry.stream, catalog_entry.tap_stream_id
    redshift_pkey = STREAMS[stream]['key_properties'][0]
    # Runner statement for fetch_uuids async function
    pendo_uuids = asyncio.run(
        fetch_uuids(stream)
    )
    LOGGER.info(
        f"CATALOG_ENTRY: {catalog_entry}{NL}"
        + f"REDSHIFT_PKEY: {redshift_pkey}{NL}"
        + f"COUNT OF PENDO_UUIDS: {len(pendo_uuids)}{NL}"
        + f"BEGINNING SYNC FOR {tap_stream_id} TABLE"
    )
    with connection.cursor() as cursor:
        schema, table = catalog_entry.table.split('.')
        params = {}
        select = 'SELECT {} FROM {}.{}'.format(','.join((f'"{col}"' for col in columns)), f'"{schema}"', f'"{table}"')
        if START_DATE is not None:
            formatted_start_date = datetime.datetime.strptime(
                START_DATE, '%Y-%m-%dT%H:%M:%SZ').astimezone()
        replication_key = metadata.to_map(catalog_entry.metadata).get((), {}).get('replication-key')
        replication_key_value = None
        bookmark_is_empty = state.get('bookmarks', {}).get(tap_stream_id) is None
        stream_version = get_stream_version(tap_stream_id, state)
        activate_version_message = messages.ActivateVersionMessage(
            stream=catalog_entry.stream,
            version=stream_version
        )
        state = bookmarks.write_bookmark(
            state, tap_stream_id, 'version', stream_version
        )
        LOGGER.debug(
            f"BOOKMARK_IS_EMPTY: {bookmark_is_empty}"
        )
        LOGGER.info(
            f"FORMATTED DATE: {formatted_start_date}{NL}"
            + f"REPLICATION_KEY: {replication_key}{NL}"
            + f"STREAM_VERSION: {stream_version}{NL}"
            + f"ACTIVATE_VERSION_MESSAGE: {activate_version_message}{NL}"
            + f"STATE: {state}"
        )
        if replication_key or bookmark_is_empty:
            yield activate_version_message
        if replication_key:
            replication_key_value = bookmarks.get_bookmark(
                state, tap_stream_id, 'replication_key_value'
            ) or formatted_start_date.isoformat()
        if replication_key_value is not None:
            entry_schema = catalog_entry.schema
            if entry_schema.properties[replication_key].format == 'date-time':
                replication_key_value = pendulum.parse(replication_key_value)
            # Building query to select only IDs returned by fetch_uuids() func
            select += ' WHERE {} > %(replication_key_value)s'.format(replication_key)
            select += ' AND {} = ANY %(pendo_uuids)s'.format(redshift_pkey)
            select += ' ORDER BY {} ASC'.format(replication_key)
            select += ' LIMIT {}'.format(QUERY_LIMIT)
            params['replication_key_value'] = replication_key_value
            params['pendo_uuids'] = (pendo_uuids,)
        elif replication_key is not None:
            select += ' WHERE {} IN %(pendo_uuids)s'.format(redshift_pkey)
            select += ' ORDER BY {} ASC'.format(replication_key)
            params['pendo_uuids'] = (pendo_uuids,)
        select_all = f'SELECT COUNT(*) FROM {schema}.{table}'
        select_all += ' WHERE {} = ANY %(pendo_uuids)s'.format(redshift_pkey)
        select_all += ' LIMIT {}'.format(QUERY_LIMIT)
        params['pendo_uuids'] = (pendo_uuids,)
        query_string_all = cursor.mogrify(select_all)
        cursor.execute(select_all, params)
        total_rows = cursor.fetchone()[0]
        volume_message = messages.VolumeMessage(
            stream=catalog_entry.stream,
            count=total_rows
        )
        yield volume_message
        LOGGER.debug(
            f"EXECUTED QUERY: {query_string_all}{NL}"
            + f"TOTAL ROWS: {total_rows}{NL}"
            + f"VOLUME_MESSAGE: {volume_message}"
        )
        time_extracted = utils.now()
        query_string = cursor.mogrify(select, params)
        cursor.execute(select, params)
        LOGGER.info(
            f"EXECUTED QUERY: {query_string}"
        )
        row = cursor.fetchone()
        rows_saved = 0
        with metrics.record_counter(None) as counter:
            counter.tags['database'] = catalog_entry.database
            counter.tags['table'] = catalog_entry.table
            while row:
                counter.increment()
                rows_saved += 1
                record_message = messages.row_to_record(
                    catalog_entry, stream_version, row, columns, time_extracted
                )
                yield record_message
                if replication_key is not None:
                    state = bookmarks.write_bookmark(
                        state,
                        tap_stream_id,
                        'replication_key_value',
                        record_message.record[replication_key]
                    )
                if rows_saved % 1000 == 0:
                    yield messages.StateMessage(
                        value=(copy.deepcopy(state)))
                row = cursor.fetchone()
        if not replication_key:
            yield activate_version_message
            yield
            state = bookmarks.write_bookmark(
                state, catalog_entry.tap_stream_id, 'version', None
            )
        yield messages.StateMessage(
            value=(copy.deepcopy(state)))


def get_stream_version(tap_stream_id=None, state=None):
    """Returns stream bookmark if exists, else creates version from time"""
    return bookmarks.get_bookmark(
        state, tap_stream_id, 'version') or int(time.time() * 1000)


def build_state(raw_state=None, catalog=None):
    """Returns State Message for selected streams
    to serve as bookmark for incremental syncs
    """
    state = {}
    currently_syncing = bookmarks.get_currently_syncing(raw_state)
    LOGGER.info(
        f"BUILDING STATE FROM RAW STATE {raw_state}{NL}" +
        f"CURRENTLY_SYNCING: {currently_syncing}"
    )
    if currently_syncing:
        state = bookmarks.set_currently_syncing(state, currently_syncing)
    for catalog_entry in catalog.streams:
        tap_stream_id = catalog_entry.tap_stream_id
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_method = catalog_metadata.get((), {}).get('replication-method')
        raw_stream_version = bookmarks.get_bookmark(
            raw_state, tap_stream_id, 'version'
        )
        if replication_method == 'INCREMENTAL':
            replication_key = catalog_metadata.get((), {}).get('replication-key')
            state = bookmarks.write_bookmark(
                state, tap_stream_id, 'replication_key', replication_key
            )
            raw_replication_key = bookmarks.get_bookmark(
                raw_state, tap_stream_id, 'replication_key'
            )
            if raw_replication_key == replication_key:
                raw_replication_key_value = bookmarks.get_bookmark(
                    raw_state, tap_stream_id, 'replication_key_value'
                )
                state = bookmarks.write_bookmark(
                    state, tap_stream_id, 'replication_key_value', raw_replication_key_value
                )
            if raw_stream_version is not None:
                state = bookmarks.write_bookmark(
                    state, tap_stream_id, 'version', raw_stream_version
                )
        elif replication_method == 'FULL_TABLE' and raw_stream_version is None:
            state = bookmarks.write_bookmark(
                state, tap_stream_id, 'version', raw_stream_version
            )
    return state
