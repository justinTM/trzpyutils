import logging
from logging import Logger
import sys


def get_logger():
    logger = Logger(__name__)
    # logger._logger.propagate = False
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(levelname)s][%(funcName)s:%(lineno)d] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
