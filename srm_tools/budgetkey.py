from sqlalchemy import create_engine
from sqlalchemy.sql import text
from conf import settings
from srm_tools.debug_cache import cache_get, cache_set

engine = create_engine(settings.BUDGETKEY_DATABASE_URL)
conn = engine.connect()



def fetch_from_budgetkey(sql):
    key = 'bk:' + sql
    data = cache_get(key)
    if data:
        return data
    result = conn.execute(text(sql))
    ret = list(map(lambda x: x._asdict(), result))
    cache_set(key, ret)
    return ret
