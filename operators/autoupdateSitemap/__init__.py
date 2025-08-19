from srm_tools.logger import logger
from srm_tools.error_notifier import invoke_on
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

def pingSiteMapForGoogle():
    sitemap_url = "https://www.kolsherut.org.il/sitemap.txt"
    ping_url = f"http://www.google.com/ping?sitemap={sitemap_url}"
    logger.info(f"Pinging Google with sitemap: {sitemap_url}")

    try:
        # Using a 'with' statement ensures the connection is closed properly
        with urlopen(ping_url) as response:
            # A status code of 200 means the request was received successfully.
            if response.getcode() == 200:
                logger.info("Successfully notified Google. Response status code:", response.getcode())
                return True
            else:
                logger.warning(f"Google responded with an unexpected status code: {response.getcode()}")
                return False

    except (URLError, HTTPError) as e:
        # This catches network errors or HTTP error codes (like 404, 503, etc.)
        logger.warning(f"Failed to ping Google. An error occurred: {e}")
        return False


def run(*_):
    logger.info('AutoUpdateSitemap starting')

    logger.info('AutoUpdateSitemap done')


def operator(*_):
    invoke_on(run, 'AutoUpdateSitemap')


if __name__ == '__main__':
    operator(None, None, None)

