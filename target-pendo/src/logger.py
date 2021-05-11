from datetime import datetime as dt
import logging


class SyncLogger:
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.log_file = dt.now().strftime("logs/redshift_pendo_%m_%d_%Y.log")
        self.log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger.propagate = False  # prevents duplicate logging in the console

        # create formatter and add it to the handlers
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(self.log_format)
        stream_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(stream_handler)

        file_handler = logging.FileHandler(self.log_file, mode='a')
        file_handler.setFormatter(self.log_format)
        file_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)

    def log_backoff(self, details):
        (_, exc, _) = sys.exc_info()
        self.logger.info(
            "Error sending data to Pendo.\n" +
            f"Sleeping {details['wait']} seconds before trying again: {exc}"
        )
