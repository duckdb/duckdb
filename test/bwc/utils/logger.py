import logging
import os
import sys

log_level = logging.INFO


def make_logger(name: str):
    ll = log_level
    logger = logging.getLogger(name)
    logger.setLevel(ll)

    if os.environ.get('CI'):
        formatter = logging.Formatter('%(message)s')

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(ll)
        stdout_handler.setFormatter(formatter)
        stdout_handler.addFilter(lambda record: record.levelno < logging.ERROR)

        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.ERROR)
        stderr_handler.setFormatter(formatter)

        logger.addHandler(stdout_handler)
        logger.addHandler(stderr_handler)
    else:
        ch = logging.StreamHandler()
        ch.setLevel(ll)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger
