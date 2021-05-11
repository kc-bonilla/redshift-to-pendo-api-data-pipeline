"""Builds Schema and associated metadata needed in catalog.json for Redshift tables"""
import json
from singer import logger
from singer import metadata
from singer.schema import Schema
from tap_redshift.streams import get_stream_props, STREAMS

LOGGER = logger.get_logger()

FLOAT_TYPES = {
    'float', 'float4', 'float8'
}
DATE_TYPES = {
    'date'
}
STRING_TYPES = {
    'char', 'character', 'nchar', 'bpchar',
    'text', 'varchar', 'character varying', 'nvarchar'
}
BYTES_FOR_INTEGER_TYPE = {
    'int2': 2, 'int': 4,
    'int4': 4, 'int8': 8
}
DATETIME_TYPES = {
    'timestamp',
    'timestamptz',
    'timestamp without time zone',
    'timestamp with time zone'
}


def catalog_from_config(stream):
    """Pulls stream properties from tap_redshift.streams"""
    stream_props = get_stream_props(stream)
    return stream_props


def schema_for_column(col):
    """Returns the Schema object for the given Column"""
    stream_list = list(STREAMS.keys())
    stream_dict = {}
    for stream in stream_list:
        stream_dict[stream] = catalog_from_config(stream)
    stream_name = stream_dict[stream].get('stream_name')
    target_pkey = stream_dict[stream].get('primary_key')
    tap_pkey = stream_dict[stream].get('field_mappings')[target_pkey]
    key_properties = stream_dict[stream].get('key_properties')

    if col['name'] == tap_pkey:
        inclusion = 'automatic'
    else:
        inclusion = 'available'
    column_type = col['type'].lower()
    column_nullable = col['nullable'].lower()
    result = Schema(inclusion=inclusion)
    if column_type == 'bool':
        result.type = 'boolean'
    elif column_type in BYTES_FOR_INTEGER_TYPE:
        result.type = 'integer'
        bits = BYTES_FOR_INTEGER_TYPE[column_type] * 8
        result.minimum = 0 - 2 ** (bits - 1)
        result.maximum = 2 ** (bits - 1) - 1
    elif column_type in FLOAT_TYPES:
        result.type = 'number'
    elif column_type == 'numeric':
        result.type = 'number'
    elif column_type in STRING_TYPES:
        result.type = 'string'
    elif column_type in DATETIME_TYPES:
        result.type = 'string'
        result.format = 'date-time'
    elif column_type in DATE_TYPES:
        result.type = 'string'
        result.format = 'date'
    else:
        result = Schema(
            None,
            inclusion='unsupported',
            description=f"Unsupported column type {column_type}"
        )
    if column_nullable == 'yes':
        result.type = ['null', result.type]
    return result


def create_column_metadata(db_name=None,
                           cols=None,
                           is_view=None,
                           table_name=None,
                           key_properties=[]):
    """Used in discovery mode to build catalog metadata"""
    stream_list = list(STREAMS.keys())
    for stream in stream_list:
        stream_dict = catalog_from_config(stream)
        stream_name = stream_dict.get('stream_name')
        target_pkey = stream_dict.get('primary_key')
        tap_pkey = stream_dict.get('field_mappings')[target_pkey]
        mdata = metadata.new()
        if table_name == stream_name:
            mdata = metadata.write(
                mdata, (), 'selected-by-default', True
            )
            mdata = metadata.write(
                mdata, (), 'selected', True
            )
            key_properties = stream_dict.get('key_properties')
        else:
            mdata = metadata.write(
                mdata, (), 'selected-by-default', False
            )
        mdata = metadata.write(
            mdata, (), 'key_properties', key_properties
        )
        mdata = metadata.write(
            mdata, (), 'is-view', is_view
        )
        mdata = metadata.write(
            mdata, (), 'schema-name', table_name
        )
        mdata = metadata.write(
            mdata, (), 'database-name', db_name
        )
        valid_rep_keys = []
        for col in cols:
            if col['type'] in DATETIME_TYPES:
                valid_rep_keys.append(col['name'])
            schema = schema_for_column(col)
            mdata = metadata.write(
                mdata,
                ('properties', col['name']),
                'selected-by-default', schema.inclusion != 'unsupported'
            )
            mdata = metadata.write(
                mdata, ('properties', col['name']),
                'sql-datatype', col['type'].lower()
            )
            mdata = metadata.write(
                mdata, ('properties', col['name']),
                'inclusion', schema.inclusion
            )
        if valid_rep_keys:
            mdata = metadata.write(
                mdata, (), 'valid-replication-keys', valid_rep_keys
            )
            if table_name in stream:
                mdata = metadata.write(
                    mdata, (), 'replication-method', 'FULL_TABLE'
                )
                mdata = metadata.write(
                    mdata, (), 'replication-key', valid_rep_keys[0]
                )
        else:
            mdata = metadata.write(
                mdata, (), 'forced-replication-method', {
                    'replication-method': 'FULL_TABLE',
                    'reason': 'No replication keys found from table'}
            )
        return metadata.to_list(mdata)
