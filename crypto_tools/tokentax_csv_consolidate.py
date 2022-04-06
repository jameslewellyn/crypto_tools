#!/usr/bin/env python3
"""Contains the main package functionality."""

import argparse
import logging
import sys


package_logger = logging.getLogger(__package__)
logger = logging.getLogger(__name__)


def main() -> None:
    """Do the main thing."""
    # Get arguments
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show DEBUG level logging messages")
    args = parser.parse_args()

    # Use root python logger to set loglevel
    if args.verbose is True:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    logger.debug("debug: got here")
    logger.info("info: got here")
    # Successful exit
    sys.exit(0)
