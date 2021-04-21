import logging
from logging.handlers import RotatingFileHandler


LOG_LEVEL = logging.DEBUG
LOG_FILENAME = '../logs/frappe.log'


def get_logger():
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s | %(name)s:\n%(message)s')

    file_handler = RotatingFileHandler(
        LOG_FILENAME, maxBytes=100000, backupCount=20)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger('heartbeat_server')
    logger.setLevel(LOG_LEVEL)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    return logger
