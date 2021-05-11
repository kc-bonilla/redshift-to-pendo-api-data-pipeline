import sys
import copy
import datetime
import dateutil
import pytz
import simplejson as json
from singer import logger, metadata, metrics, utils
from tap_redshift import resolve
from tap_redshift import discover
from tap_redshift import sync
from tap_redshift import bookmarks


LOGGER = logger.get_logger()


class Message(object):
    """Base class for messages"""

    def asdict(self):
        raise Exception("Not implemented")

    def __eq__(self, other):
        return isinstance(other, Message) and self.asdict() == other.asdict()

    def __repr__(self):
        pairs = [f"{key}={val}" for key, val in self.asdict().items()]
        attrstr = ", ".join(pairs)
        return f"{self.__class__.__name__}({attrstr})"

    def __str__(self):
        return str(self.asdict())


class RecordMessage(Message):
    """The RECORD message has these fields:
      * stream (string) - The name of the stream the record belongs to.
      * record (dict) - The raw data for the record
      * version (optional, int) - For versioned streams, the version number
    >>> msg = RecordMessage(
    >>>     stream='users',
    >>>     record={'id': 1, 'name': 'Mary'})
    """

    def __init__(self, stream, record, version=None, time_extracted=None):
        self.stream = stream
        self.record = record
        self.version = version
        self.time_extracted = time_extracted
        if time_extracted and not time_extracted.tzinfo:
            raise ValueError(
                "'time_extracted' must be either None " +
                "or an aware datetime (with a time zone)"
            )

    def asdict(self):
        result = {
            'type': 'RECORD',
            'stream': self.stream,
            'record': self.record,
        }
        if self.version is not None:
            result['version'] = self.version
        if self.time_extracted:
            as_utc = self.time_extracted.astimezone(pytz.utc)
            result['time_extracted'] = as_utc.strftime(utils.DATETIME_FMT)
        return result
        
    def __str__(self):
        return str(self.asdict())


class SchemaMessage(Message):
    """The SCHEMA message has these fields:
      * stream (string) - The name of the stream this schema describes.
      * schema (dict) - The JSON schema.
      * key_properties (list of strings) - List of primary key properties.
    >>> msg = SchemaMessage(
    >>>     stream='users',
    >>>     schema={'type': 'object',
    >>>             'properties': {
    >>>                 'id': {'type': 'integer'},
    >>>                 'name': {'type': 'string'}
    >>>             }
    >>>            },
    >>>     key_properties=['id'])
    """
    def __init__(self, stream, schema, key_properties, bookmark_properties=None):
        self.stream = stream
        self.schema = schema
        self.key_properties = key_properties

        if isinstance(bookmark_properties, (str, bytes)):
            bookmark_properties = [bookmark_properties]
        if bookmark_properties and not isinstance(bookmark_properties, list):
            raise Exception(
                "bookmark_properties must be a string or list of strings"
            )

        self.bookmark_properties = bookmark_properties

    def asdict(self):
        result = {
            'type': 'SCHEMA',
            'stream': self.stream,
            'schema': self.schema,
            'key_properties': self.key_properties
        }
        if self.bookmark_properties:
            result['bookmark_properties'] = self.bookmark_properties
        return result


class VolumeMessage(Message):
    """The Volume message has two fields:
    * stream (string) - The name of the stream this schema describes.
    * count (int) - The count of the RECORD messages returned from tap invocation.
    >>> msg = VolumeMessage(
                    stream='users',
                    count= 10000
                )
    """
    def __init__(self, stream, count):
        self.stream = stream
        self.count = count

    def asdict(self):
        return {
            'type': 'VOLUME',
            'stream': self.stream,
            'count': self.count
        }


class StateMessage(Message):
    """STATE message
    The STATE message has one field:
      * value (dict) - The value of the state.
    >>> msg = StateMessage(
    >>>     value={'users': '2017-06-19T00:00:00'})
    """
    def __init__(self, value):
        self.value = value

    def asdict(self):
        return {
            'type': 'STATE',
            'value': self.value
        }


class ActivateVersionMessage(Message):
    """ACTIVATE_VERSION messages has these fields:
      * stream - The name of the stream.
      * version - The version number to activate.
    This is a signal to the Target that it should delete all previously
    seen data and replace it with all the RECORDS it has seen where the
    record's version matches this version number.
    >>> msg = ActivateVersionMessage(
    >>>     stream='users',
    >>>     version=2
        )
    """
    def __init__(self, stream, version):
        self.stream = stream
        self.version = version

    def asdict(self):
        return {
            'type': 'ACTIVATE_VERSION',
            'stream': self.stream,
            'version': self.version
        }


def _required_key(msg, key):
    """checks message for a required 'type' key"""
    if key not in msg:
        raise Exception(
            f"Message is missing required key '{key}': {msg}"
        )
    return msg[key]


def parse_message(msg):
    """Parse a message string into a Message object"""
    obj = json.loads(msg)
    msg_type = _required_key(obj, 'type')
    if msg_type == 'VOLUME':
        return VolumeMessage(
            stream=_required_key(obj, 'stream'),
            count=_required_key(obj, 'count')
        )
    elif msg_type == 'RECORD':
        time_extracted = obj.get('time_extracted')
        if time_extracted:
            time_extracted = dateutil.parser.parse(time_extracted)
        return RecordMessage(
            stream=_required_key(obj, 'stream'),
            record=_required_key(obj, 'record'),
            version=obj.get('version'),
            time_extracted=time_extracted
        )
    elif msg_type == 'SCHEMA':
        return SchemaMessage(
            stream=_required_key(obj, 'stream'),
            schema=_required_key(obj, 'schema'),
            key_properties=_required_key(obj, 'key_properties'),
            bookmark_properties=obj.get('bookmark_properties')
        )
    elif msg_type == 'STATE':
        return StateMessage(
            value=_required_key(obj, 'value')
        )
    elif msg_type == 'ACTIVATE_VERSION':
        return ActivateVersionMessage(
            stream=_required_key(obj, 'stream'),
            version=_required_key(obj, 'version')
        )
    else:
        raise Exception(
            "Message Type is not supported"
        )
    return


def format_message(message):
    """returns a message formatted as a dict from JSON object"""
    return json.dumps(message.asdict(), use_decimal=True)


def write_message(message):
    """writes message to sys.stdout stream"""
    sys.stdout.write(format_message(message) + '\n')
    sys.stdout.flush()


def write_record(stream_name, record, stream_alias=None, time_extracted=None):
    """Write a single record for the given stream.
    >>> write_record("users", {"id": 2, "email": "mike@foreground.com"})
    """
    write_message(RecordMessage(
        stream=(stream_alias or stream_name),
        record=record,
        time_extracted=time_extracted)
    )


def write_records(stream_name, records):
    """Write a list of records for the given stream.
    >>> chris = {"id": 1, "email": "chris@foreground.com"}
    >>> mike = {"id": 2, "email": "mike@foreground.com"}
    >>> write_records("users", [chris, mike])
    """
    for record in records:
        write_record(stream_name, record)


def write_schema(stream_name, schema, key_properties, bookmark_properties=None, stream_alias=None):
    """Write a schema message.
    >>> stream = 'test'
    >>> schema = {'properties': {'id': {'type': 'integer'}, 'email': {'type': 'string'}}}
    >>> key_properties = ['id']
    >>> write_schema(stream, schema, key_properties)
    """
    if isinstance(key_properties, (str, bytes)):
        key_properties = [key_properties]
    if not isinstance(key_properties, list):
        raise Exception(
            "key_properties must be a string or list of strings"
        )
    write_message(
        SchemaMessage(
            stream=(stream_alias or stream_name),
            schema=schema,
            key_properties=key_properties,
            bookmark_properties=bookmark_properties
        )
    )


def write_state(value):
    """Write a state message.
    >>> write_state({'last_updated_at': '2017-02-14T09:21:00'})
    """
    write_message(
        StateMessage(value=value)
    )


def write_volume(stream_name, count):
    """Write a message for the volume of records.
    >>> stream = 'test'
    >>> count = 10000
    >>> write_volume(stream, count)
    """
    write_message(
        VolumeMessage(stream_name, count)
    )


def write_version(stream_name, version):
    """Write an activate version message.
    >>> stream = 'test'
    >>> version = int(time.time())
    >>> write_version(stream, version)
    """
    write_message(
        ActivateVersionMessage(stream_name, version)
    )


def generate_messages(conn, db_schema, catalog, state):
    """Controls generation of State and Schema Messages
        for 'Selected' tables in catalog.json"""
    catalog = resolve.resolve_catalog(discover.discover_catalog(conn, db_schema),
                                      catalog, state)
    for catalog_entry in catalog.streams:
        state = bookmarks.set_currently_syncing(state, catalog_entry.tap_stream_id)
        catalog_md = metadata.to_map(catalog_entry.metadata)
        if catalog_md.get((), {}).get('is-view'):
            key_properties = catalog_md.get((), {}).get('view-key-properties')
        else:
            key_properties = catalog_md.get((), {}).get('table-key-properties')
        bookmark_properties = catalog_md.get((), {}).get('replication-key')
        # Emit a state message to indicate that we've started this stream
        yield StateMessage(value=copy.deepcopy(state))
        # Emit a SCHEMA message before we sync any records
        yield SchemaMessage(
            stream=catalog_entry.stream,
            schema=catalog_entry.schema.to_dict(),
            key_properties=key_properties,
            bookmark_properties=bookmark_properties)
        # Emit a RECORD message for each record in the result set
        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = catalog_entry.database
            timer.tags['table'] = catalog_entry.table
            for message in sync.sync_table(conn, catalog_entry, state):
                yield message
    # finished processing all streams, so clear
    # currently_syncing from the state and emit a state message.
    state = bookmarks.set_currently_syncing(state, None)
    yield StateMessage(value=copy.deepcopy(state))


def row_to_record(catalog_entry, version, row, columns, time_extracted):
    """Function for writing table rows to stream Record Messages"""
    row_to_persist = ()
    for idx, elem in enumerate(row):
        if isinstance(elem, datetime.date):
            elem = elem.isoformat('T') + 'Z'
        row_to_persist += (elem,)
    return RecordMessage(
        stream=catalog_entry.stream,
        record=dict(zip(columns, row_to_persist)),
        version=version,
        time_extracted=time_extracted
    )
