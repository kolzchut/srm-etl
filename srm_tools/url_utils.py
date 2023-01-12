import re

GOOD_DOMAIN = re.compile(r'^[a-z0-9][a-z0-9\-\.]{0,61}[a-z0-9]\.[a-z]{2,}$', re.IGNORECASE)

def fix_url(url):
    if not url or not isinstance(url, str):
        return
    if url.startswith('http'):
        return url
    if GOOD_DOMAIN.match(url):
        return 'http://' + url
    return


if __name__ == '__main__':
    # Tests
    assert fix_url('https://www.google.com') == 'https://www.google.com'
    assert fix_url('http://www.google.com') == 'http://www.google.com'
    assert fix_url('www.google.com') == 'http://www.google.com'
    assert fix_url('google.com') == 'http://google.com'
    assert fix_url('google') is None
    assert fix_url('google.com.') is None
    assert fix_url('google.com-') is None
    
    