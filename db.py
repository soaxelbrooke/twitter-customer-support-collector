from datetime import datetime
from typing import Optional, List

import psycopg2
from psycopg2.extras import NamedTupleConnection, execute_values
import toolz
from twitter import Status, User

from fetch import ApiRequest


@toolz.memoize
def db_conn():
    return psycopg2.connect(dbname='twitter_cs', connection_factory=NamedTupleConnection)


LAST_TWEET_QUERY = """
    SELECT "data" FROM tweets 
    WHERE "data" ->> 'user' ->> 'screen_name' ILIKE %s
    ORDER BY created_at DESC
    LIMIT 1;
"""


def get_last_tweet(screen_name: str) -> Optional[Status]:
    """ Attempts to fetch the last seen tweet from the provided screen name """
    conn = db_conn()
    crs = conn.cursor()
    crs.execute(LAST_TWEET_QUERY, (screen_name,))
    results = crs.fetchall()
    return results[0] if len(results) > 0 else None


def user_to_record(user: User) -> tuple:
    """ Converts a user to a record that can be saved in postgres """
    return str(user.id), user.AsJsonString()


def save_users(users: List[User]):
    """ Saves users pulled from tweets """
    unique_users = [*toolz.unique(users, key=lambda u: u.id)]
    conn = db_conn()
    crs = conn.cursor()
    execute_values(crs, """
                        INSERT INTO users (user_id, data) VALUES %s
                        ON CONFLICT (user_id) DO UPDATE SET
                          data = EXCLUDED.data,
                          observed_at = (now() at time zone 'utc');""",
                   [*map(user_to_record, unique_users)])
    conn.commit()


def tweet_to_record(tweet: Status) -> tuple:
    """ Converts a tweet to a record that can be saved in postgres """
    return str(tweet.id), datetime.fromtimestamp(tweet.created_at_in_seconds), tweet.AsJsonString()


def save_tweets(tweets: List[Status]):
    """ Saves a list of tweets to postgres """
    save_users([t.user for t in tweets])
    conn = db_conn()
    crs = conn.cursor()
    execute_values(crs, """
                      INSERT INTO tweets (status_id, created_at, data) 
                      VALUES %s
                      ON CONFLICT (status_id) DO UPDATE SET
                        created_at = EXCLUDED.created_at,
                        data = EXCLUDED.data,
                        observed_at = (now() at time zone 'utc');
                   """, [*map(tweet_to_record, tweets)])
    conn.commit()


def save_request(request: ApiRequest):
    """ Saves an API request to postgres """
    conn = db_conn()
    crs = conn.cursor()
    crs.execute("INSERT INTO requests (screen_name, kind) VALUES (%s, %s);", request)
    conn.commit()


def prioritize_screen_names(screen_names: List[str]) -> List[str]:
    """ Re-orders provided screen names by collection priority.  Can be based on inferred volume,
        time since last collect, and other metadata.
    """
    conn = db_conn()
    crs = conn.cursor()
    crs.execute("""SELECT DISTINCT ON (screen_name) * 
                   FROM requests
                   ORDER BY screen_name, created_at DESC NULLS LAST;""")

    requests = {r.screen_name: r.created_at.timestamp() for r in crs.fetchall()}
    return sorted(screen_names, key=lambda sn: requests.get(sn.strip('@').lower(), 0))
