import smtplib
import traceback

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from conf import settings
from srm_tools.logger import logger

def send_failure_email(operation_name: str, error: str, is_test: bool = False,reraise: bool = True):
    ENV_NAME = settings.ENV_NAME
    SMTP_SERVER = settings.SMTP_SERVER
    SMTP_PORT = settings.SMTP_PORT
    SENDER_EMAIL = settings.EMAIL_NOTIFIER_SENDER_EMAIL
    SENDER_PASSWORD = settings.EMAIL_NOTIFIER_PASSWORD
    RECIPIENT_LIST = settings.EMAIL_NOTIFIER_RECIPIENT_LIST

    RECIPIENT_LIST.append(SENDER_EMAIL)
    if is_test:
        logger.info("send_failure_email triggered")
        logger.info(f"sender email: {SENDER_EMAIL}.")
        logger.info(f"recipient list: {RECIPIENT_LIST}.")
    
    subject = f"ETL Task - {ENV_NAME} : {operation_name} Failed"
    body = f"Operation `{operation_name}` encountered an error:\n\nError Log:\n{error}"

    # Create email message
    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(RECIPIENT_LIST)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        # Connect to SMTP server
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()  # Secure connection
        server.login(SENDER_EMAIL, SENDER_PASSWORD)  # Authenticate
        server.sendmail(SENDER_EMAIL, RECIPIENT_LIST, msg.as_string())
        server.quit()
        logger.info(f"Email sent about {operation_name}.")
    except Exception as e:
        logger.info(f"Failed to send email: {e}, {SENDER_EMAIL},{SENDER_PASSWORD}")
        raise Exception(f"Failed to send email:{e}. \nOperation {operation_name} encountered an error. \nERROR:{error}")
    
    if not is_test and reraise:
        raise # Re-raise the original exception


def invoke_on(func, name, is_test=False, on_success=None, on_failure=None):
    try:
        func()
        if on_success:
            on_success()
    except Exception as e:
        logger.info(f"Error in {name}: {e}")
        if on_failure:
            on_failure()
        send_failure_email(name, traceback.format_exc(), is_test)
    except BaseException as e:
        logger.info(f"BaseException caught: {e}")
        send_failure_email(name, traceback.format_exc(), is_test)
