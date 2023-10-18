import os
import json
from srm_tools.hash import hasher

DEBUG_CACHE = os.environ.get('SRM_DEBUG_CACHE', '0') == '1'
os.makedirs('.cache', exist_ok=True)

def get_path(key):
    key = hasher(key)
    parts = key[:2], key[2:4], key[4:]
    dirs = os.path.join('.cache', *parts[:-1])
    os.makedirs(dirs, exist_ok=True)
    return os.path.join('.cache', *parts)


def cache_set(key, data):
    if not DEBUG_CACHE:
        return
    path = get_path(key)
    with open(path, 'w') as f:
        json.dump(data, f)


def cache_get(key):
    if not DEBUG_CACHE:
        return None
    path = get_path(key)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)