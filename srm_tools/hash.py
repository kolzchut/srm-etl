import hashlib


def hasher(*args):
    return hashlib.sha1(''.join(filter(None, args)).encode('utf-8')).hexdigest()[:8]

