# -*- coding: utf-8 -*-
import logging
import os
import sys

def setup_logging():
    """
    Configure basic stdout logging for the application.
    """
    fmt = "%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s"
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format=fmt,
        handlers=[logging.StreamHandler(sys.stdout)],
    )