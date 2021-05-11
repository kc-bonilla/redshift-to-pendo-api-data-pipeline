from singer import logger, utils
from singer.catalog import Catalog
import simplejson as json
import argparse

LOGGER = logger.get_logger()
REQUIRED_CONFIG_KEYS = [
    'host',
    'dbname',
    'user',
    'password',
    'start_date',
    'schema',
    'target_integration_key'
]


def parse_args(required_config_keys=None):
    """Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -l,--limit      Query Limit
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '-s', '--state',
        help='State file')

    parser.add_argument(
        '-d', '--discover',
        action='store_true',
        help='Do schema discovery')

    parser.add_argument(
        '-l', '--limit',
        type=int,
        help='Constraint on Rows Returned from Tap Query')

    parser.add_argument(
        '--catalog',
        help='Catalog file')

    args = parser.parse_args()
    # sets schema in config file if given, otherwise default to 'public' if not provided
    # parse required config args from tap config file
    # setting defaults
    # return {'CONFIG': CONFIG, 'QUERY LIMIT': query_limit}
    if args.config:
        setattr(args, 'tap_config_path', args.config)
        args.config = load_json(args.config)
    if args.state:
        setattr(args, 'state_path', args.state)
        args.state = load_json(args.state)
    else:
        args.state = {}
    if args.catalog:
        setattr(args, 'catalog_path', args.catalog)
        args.catalog = Catalog.load(args.catalog)

    check_config(args.config, required_config_keys)
    return args


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception(
            f"Config is missing required keys: {missing_keys}"
        )


def load_json(path):
    with open(path) as file:
        return json.load(file)


args = parse_args(required_config_keys=REQUIRED_CONFIG_KEYS)
start_date = args.config.get('start_date')
target_int_key = args.config.get('target_integration_key')
catalog = args.catalog
args_config = dict(args.config)
db_schema = args_config.get('schema', 'public')  # Sets schema if given, default 'public'
query_limit = args.limit if args.limit else 1000000
