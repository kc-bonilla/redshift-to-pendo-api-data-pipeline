## Recent Changes (reflected in attached code)
• Created StreamProps class from which stream_dict objects are created for each incoming stream, allowing for aggregation and isolation of stream properties, counts, results, etc. for each stream.
• Reconfigured target_config.json and target_pendo/__init__.py to allow for looping through multiple streams in single sync.
• Refactored async function fetch_uuids in tap_redshift/sync.py module to be more dynamic/succinct by using the target (Pendo) entity's attributes from the tap_redshift/streams.py module.
• A small mod but important note: the key reserved_field_overrides used in target_config.json to map tap (redshift) fields to target (Pendo) fields was too long and an inept description for its content/function. It is now replaced with field_mappings in target_config.json and all tap/target modules.
• Removed endpoints.py as standalone module and added to target_pendo/__init__.py module.
• Added asyncio.Sempahore() and a wrapper/utility function to implement the semaphore on the task flow of tasks sent to async post_request() to try and help limit the concurrent requests to the Pendo client, which at times results in the response for 1 or 2 of the last requests getting hung up and dropping during a FULL_TABLE replication. I wasn’t seeing a difference but there is likely a small tweak to correct it and make it effective.
• Replaced module target_pendo.py with __init__.py in correspondence with best practice for python packages to resolve occasional issue of Import Error due to a naming redundancy between the module target_pendo.py and the package target_pendo
• The binary executable depends upon an entry point as defined in the ~/.virtualenvs/targetpendo/src/target-pendo/target_pendo.egg-info directory.
• Executing the binary file after renaming the module must then have that change reflected in its entry point designation by making the following modification:
 [console_scripts]
 target-pendo = target_pendo.target_pendo:main__init__:main
• Added verbose and quiet logging options as CLI arguments.
o	The verbose option sets the logging level to DEBUG.
o	The quiet option sets the logging level to WARNING.
o	The default if neither option is provided is INFO.
• Added logger.py module with a SyncLogger class that has a formatted StreamHandler and FileHandler attached to the main logger so that sys.stdout is still shown in the console/terminal, while all logging is also captured and saved as a .log file, named by date.
o The FileHandler mode is set to append (mode = 'a'), to prevent dates with multiple runs from overwriting logs from an earlier run.
• Added the directory ~/logs in EC2 home directory (/home/kcbonilla/) to store the logs that are produced by each run. 

example:
redshift_pendo_04_24_2021.log
• Miscellaneous refactoring, standardization, and styling changes across tap and target modules
• Exception handling enhancements/mods

## TODO
• Setup of S3 IAM permissions to upload and download from the a given bucket so that config files and/or logfiles can be passed before and after executions.
• Needs policy/process for emptying ~/tap-redshift/logs/ folder with execution logs in EC2; the log files are hundreds of thousands of lines of text, so they accumulate memory quickly.
• The last_updated field in Redshift still needs to be made live so that when a user has some attribute change in Redshift, their last_updated field updates to the timestamp of the attribute’s change and incremental syncs can then pick-up just these modifications.
• Had some trouble getting the executables setup in the EC2 virtualenvs bin folders, so I have been directly calling the __init__.py modules of the tap and target in the pipe command. This shouldn’t be too difficult to fix if you look through the load_entry_points and egg-link info in each virtualenv. They are both configured incorrectly in EC2 right now.
