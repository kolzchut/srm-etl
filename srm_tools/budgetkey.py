from sqlalchemy import create_engine
from conf import settings

def fetch_from_budgetkey(sql):
    engine = create_engine(settings.BUDGETKEY_DATABASE_URL)
    with engine.connect() as conn:
        result = conn.execute(sql)
        return map(dict, result)