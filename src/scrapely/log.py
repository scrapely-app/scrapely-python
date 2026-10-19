"""Logging for Scrapely Actors.

Provides ``ActorLogFormatter`` and ``logger``; imports of ``apify.log`` are
rewritten to ``scrapely.log`` by the source transform.
"""

from __future__ import annotations

import logging

from crawlee._log_config import CrawleeLogFormatter, configure_logger, get_configured_log_level

# Name of the logger used throughout the library (resolves to 'scrapely')
logger_name = __name__.split('.')[0]

# Logger used throughout the library
logger = logging.getLogger(logger_name)


class ActorLogFormatter(CrawleeLogFormatter):  # noqa: D101 (Inherited from parent class)
    pass


def _configure_logging() -> None:
    """Set up the default Actor log output (called by ``Actor.init`` unless ``configure_logging=False``)."""
    level = get_configured_log_level()

    # Keep the API client logger quiet unless debug logging is requested
    client_logger = logging.getLogger('scrapely_client')
    configure_logger(client_logger, remove_old_handlers=True)
    client_logger.setLevel(logging.INFO if level > logging.DEBUG else level)

    # Silence HTTPX logger unless debug logging is requested
    logging.getLogger('httpx').setLevel(logging.WARNING if level > logging.DEBUG else level)

    # Use configured log level for the SDK logger
    sdk_logger = logging.getLogger(logger_name)
    configure_logger(sdk_logger, remove_old_handlers=True)
    sdk_logger.setLevel(level)
