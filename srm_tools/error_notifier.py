import smtplib
import traceback
import json
import os

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from conf import settings
from srm_tools.logger import logger

def get_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "configuration.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)

def send_failure_email(operation_name: str, error: str, is_test: bool = False):
    config = get_config()

    ENV_NAME = settings.ENV_NAME
    EMAIL_CONFIG = config["errorNotifier"]
    SMTP_SERVER = EMAIL_CONFIG["SMTP_SERVER"]
    SMTP_PORT = EMAIL_CONFIG["SMTP_PORT"]
    SENDER_EMAIL = settings.EMAIL_NOTIFIER_SENDER_EMAIL
    SENDER_PASSWORD = settings.EMAIL_NOTIFIER_PASSWORD
    RECIPIENT_LIST = settings.EMAIL_NOTIFIER_RECIPIENT_LIST

    if is_test:
        RECIPIENT_LIST.append(SENDER_EMAIL)
        logger.info("send_failure_email triggered")
        logger.info(f"sender email: {SENDER_EMAIL}.")
        logger.info(f"recipient list: {RECIPIENT_LIST}.")
        print("send_failure_email triggered")
        print(f"sender email: {SENDER_EMAIL}.")
        print(f"recipient list: {RECIPIENT_LIST}.")
    
    subject = f"ETL Task Failed - {ENV_NAME}:{operation_name}"
    body = f"Operation `{operation_name}` encountered an error:\n\n{error}"

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
        print(f"Email sent for {operation_name}.")
        logger.info(f"Email sent for {operation_name}.")
    except Exception as e:
        print(f"Failed to send email: {e}")
        logger.info(f"Failed to send email: {e}")

def invoke_on(func, name, is_test=False, on_success=None, on_failure=None):
    try:
        func()
        if on_success:
            on_success()
    except Exception as e:
        logger.info(f"Error in {name}: {e}")
        print(f"Error in {name}: {e}")
        if on_failure:
            on_failure()
        send_failure_email(name, traceback.format_exc(), is_test)
    except BaseException as e:
        logger.info(f"BaseException caught: {e}")  # Catch anything else
        print(f"BaseException caught: {e}")  # Catch anything else
