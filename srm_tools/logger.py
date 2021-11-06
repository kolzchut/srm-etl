import logging

MODULES = [
    'requests',
    'urllib3',
    'boto',
    'botocore',
    'boto3',
    's3transfer',
    's3transfer.utils',
    's3transfer.tasks',
]
for module in MODULES:
    logging.getLogger(module).setLevel(logging.INFO)

logger = logging.getLogger('SRM')
logger.setLevel(logging.DEBUG)
