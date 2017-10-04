from typing import Optional

import psycopg2
from psycopg2.extras import NamedTupleConnection
import toolz
from twitter import Status


@toolz.memoize
def db_conn():
    return psycopg2.connect(dbname='twitter_cs', connection_factory=NamedTupleConnection)


LAST_TWEET_QUERY = """
    SELECT "data" FROM tweets 
    WHERE "data" ->> 'user' ->> 'screen_name' ILIKE %s
    ORDER BY 
"""


def get_last_tweet(screen_name: str) -> Optional[Status]:
    """ Attempts to fetch the last seen tweet from the provided screen name """
    conn = db_conn()
    crs = conn.cursor()
    crs.execute(LAST_TWEET_QUERY, (screen_name, ))


