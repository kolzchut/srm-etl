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
    'airflow.task',
]
for module in MODULES:
    logging.getLogger(module).setLevel(logging.WARNING)

logger = logging.getLogger('SRM')
logger.setLevel(logging.DEBUG)
