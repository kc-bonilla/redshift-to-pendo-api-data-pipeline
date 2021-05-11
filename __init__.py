import arrow
from singer import utils, logger
from singer.catalog import Catalog, CatalogEntry
from tap_redshift import connect, discover, messages, parsed_args, sync

LOGGER = logger.get_logger()
ARGS = parsed_args.args  # Import parsed config args from tap config file
CONFIG = parsed_args.args_config
CATALOG = parsed_args.catalog
SCHEMA = parsed_args.db_schema
STATE = parsed_args.state
RUN_START = arrow.get().format("YYYY-MM-DD HH:mm:ss.SSSSZZ")
NL = "\n"  # adding newline constant for easier multiline logging


def main_impl():
    LOGGER.info(
        f"INVOKING TAP-REDSHIFT @ {RUN_START}{NL}" +
        f"TAP-REDSHIFT ARGS: {ARGS}"
    )
    connection = connect.open_connection(CONFIG)  # Establish connection with db
    # If discover option in execution, discover db's schema/metadata for catalog
    # If catalog arg given w/o discover option, set state & sync using catalog config
    if DISCOVER:
        discover.do_discover(connection, SCHEMA)
    elif CATALOG:
        LOGGER.debug(f"CATALOG: {CATALOG}, CONNECTION: {connection}")
        state = sync.build_state(STATE, CATALOG)
        sync.do_sync(connection, SCHEMA, CATALOG, state)
    else:
        LOGGER.info("Missing required arguments")


@utils.handle_top_exception(LOGGER)
def main():
    main_impl()


if __name__ == "__main__":
    main()
