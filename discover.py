from itertools import groupby
from singer import logger, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from tap_redshift.connect import select_all
from tap_redshift.schema import schema_for_column, create_column_metadata, catalog_from_config

LOGGER = logger.get_logger()


def do_discover(conn, db_schema):
    LOGGER.info("Running discover")
    discover_catalog(conn, db_schema).dump()
    LOGGER.info("Completed discover")


def discover_catalog(conn, db_schema):
    """Returns a Catalog describing the structure of the database."""
    
    table_spec = select_all(
        conn,
        """
        SELECT table_name, table_type
        FROM INFORMATION_SCHEMA.Tables
        WHERE table_schema = '{}'
        """.format(db_schema))

    column_specs = select_all(
        conn,
        """
        SELECT c.table_name, c.ordinal_position, c.column_name, c.udt_name,
        c.is_nullable
        FROM INFORMATION_SCHEMA.Tables t
        JOIN INFORMATION_SCHEMA.Columns c ON c.table_name = t.table_name
        WHERE t.table_schema = '{}'
        ORDER BY c.table_name, c.ordinal_position
        """.format(db_schema))

    pk_specs = select_all(
        conn,
        """
        SELECT kc.table_name, kc.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kc
            ON kc.table_name = tc.table_name AND
               kc.table_schema = tc.table_schema AND
               kc.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY' AND
              tc.table_schema = '{}'
        ORDER BY
          tc.table_schema,
          tc.table_name,
          kc.ordinal_position
        """.format(db_schema))

    entries = []
    table_columns = [
        {'name': k,
         'columns': [{
             'pos': t[1],
             'name': t[2],
             'type': t[3],
             'nullable': t[4]
         } for t in v]} for k, v in groupby(column_specs, key=lambda t: t[0])]
                     
    table_pks = {
        k: [t[1] for t in v] for k, v in groupby(pk_specs, key=lambda t: t[0])
    }
                 
    table_types = dict(table_spec)
    for items in table_columns:
        table_name = items['name']
        qualified_table_name = f"{db_schema}.{table_name}"
        cols = items['columns']
        schema = Schema(
            type='object',
            properties={col['name']: schema_for_column(col) for col in cols}
        )
        key_properties = [
            col for col in table_pks.get(table_name, [])
            if schema.properties[col].inclusion != 'unsupported'
            ]
        is_view = table_types.get(table_name) == 'VIEW'
        db_name = conn.get_dsn_parameters()['dbname']
        metadata = create_column_metadata(
            db_name, cols, is_view, table_name, key_properties
        )
        tap_stream_id = f"{db_name}.{qualified_table_name}"
        entry = CatalogEntry(
            tap_stream_id=tap_stream_id,
            stream=table_name,
            schema=schema,
            table=qualified_table_name,
            metadata=metadata
        )
        entries.append(entry)
    return Catalog(entries)
