import re
import requests

cookie_re = re.compile("document.cookie='([^;]+); path=/'")

def overcome_blocking(session: requests.Session, get_resp):
    resp = get_resp()
    data = resp.content
    if len(data) < 200:
        decoded = data.decode('ascii')
        found_cookies = cookie_re.findall(decoded)
        if len(found_cookies) > 0:
            found_cookies = found_cookies[0]
            session.cookies.set(found_cookies[0], found_cookies[1], path='/')
            resp = get_resp()
    return resp