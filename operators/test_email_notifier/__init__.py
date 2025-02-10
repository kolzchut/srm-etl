from srm_tools.logger import logger
from srm_tools.error_notifier import invoke_on

def raise_exception():
    logger.info('Testing Email Notifier')
    raise Exception('Testing Email Notifier')
    

def operator(*_):
    invoke_on(raise_exception, 'email_notifier', True)

if __name__ == '__main__':
    invoke_on(raise_exception, 'email_notifier', True)
