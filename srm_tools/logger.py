import logging

logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

logger = logging.getLogger('SRM')
logger.setLevel(logging.DEBUG)
