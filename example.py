import logging

from common.logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

logger.info("Test")
