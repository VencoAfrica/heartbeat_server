import logging
import pathlib
import sys
from logging.handlers import RotatingFileHandler


LOG_LEVEL = logging.DEBUG
LOG_FILENAME = './logs/app.log'


def get_logger():
    logger = logging.getLogger('heartbeat_server')
    if not len(logger.handlers):
        formatter = logging.Formatter('[%(levelname)s] %(asctime)s | %(name)s:\n%(message)s\n')

        try:
            folder = pathlib.Path(LOG_FILENAME).parent
            folder.mkdir(parents=True, exist_ok=True)
        except FileExistsError:
            pass
        file_handler = RotatingFileHandler(
            LOG_FILENAME, maxBytes=100000, backupCount=20)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        logger.setLevel(LOG_LEVEL)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.propagate = False

    return logger
