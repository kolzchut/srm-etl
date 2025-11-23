import os
import base64
import requests
import datetime
from srm_tools.logger import logger
from srm_tools.error_notifier import invoke_on


def configs():
    GITHUB_TOKEN = os.getenv("KZ_GITHUB_TOKEN")
    REPO_OWNER = "kolzchut"
    REPO_NAME = "kolsherut-backup"
    BRANCH = "backup-request"
    FILE_PATH = "LAST_PUSH_FROM_ETL.txt"
    COMMIT_MESSAGE = "Automated commit from ETL process"
    FILE_CONTENT = datetime.datetime.now(datetime.UTC).isoformat()
    API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{FILE_PATH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    return GITHUB_TOKEN, REPO_OWNER, REPO_NAME, BRANCH, FILE_PATH, COMMIT_MESSAGE, FILE_CONTENT, API_URL, headers


def run(*_):
    logger.info("Starting Github Backup Operator")
    GITHUB_TOKEN, REPO_OWNER, REPO_NAME, BRANCH, FILE_PATH, COMMIT_MESSAGE, FILE_CONTENT, API_URL, headers = configs()

    if not GITHUB_TOKEN:
        logger.error("KZ_GITHUB_TOKEN environment variable not set; aborting GitHub backup.")
        return

    try:
        response = requests.get(API_URL + f"?ref={BRANCH}", headers=headers, timeout=30)
    except requests.RequestException as e:
        logger.error(f"❌ Error during GET request: {e}")
        return

    if response.status_code == 200:
        sha = response.json().get("sha")
    else:
        sha = None
        logger.info(f"File does not exist yet (status {response.status_code}); will create new file.")

    payload = {
        "message": COMMIT_MESSAGE,
        "content": base64.b64encode(FILE_CONTENT.encode()).decode(),
        "branch": BRANCH
    }

    if sha:
        payload["sha"] = sha

    try:
        r = requests.put(API_URL, json=payload, headers=headers, timeout=30)
    except requests.RequestException as e:
        logger.error(f"❌ Error during PUT request: {e}")
        return

    if r.status_code in [200, 201]:
        logger.info("✅ File committed successfully!")
    else:
        logger.error(f"❌ Error: {r.status_code} {r.text}")

def operator(*_):
    invoke_on(run, 'GitHub Backup Operator')


if __name__ == '__main__':
    run()
