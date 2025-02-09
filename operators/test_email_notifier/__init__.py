from srm_tools.logger import logger
from srm_tools.error_notifier import invoke_on


def operator(*_):
    logger.info('Testing Email Notifier')
    raise Exception('Testing Email Notifier')



if __name__ == '__main__':
    invoke_on(lambda: operator(), 'email_notifier', True)
