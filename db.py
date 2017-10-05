from datetime import datetime
from typing import Optional, List, Dict

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
    execute_values(crs, """INSERT INTO users (user_id, data) VALUES %s ON CONFLICT DO NOTHING;""",
                   [*map(user_to_record, unique_users)])
    conn.commit()


def tweet_to_record(tweet: Status) -> tuple:
    """ Converts a tweet to a record that can be saved in postgres """
    return str(tweet.id), datetime.fromtimestamp(tweet.created_at_in_seconds), tweet.AsJsonString()


def save_tweets(tweets: List[Status]):
    """ Saves a list of tweets to postgres """
    save_users([t.user for t in tweets])
    unique_tweets = [*toolz.unique(tweets, key=lambda t: t.id)]
    conn = db_conn()
    crs = conn.cursor()
    execute_values(crs, """INSERT INTO tweets (status_id, created_at, data)
                           VALUES %s ON CONFLICT DO NOTHING;""",
                   [*map(tweet_to_record, unique_tweets)])
    conn.commit()


def save_request(request: ApiRequest):
    """ Saves an API request to postgres """
    conn = db_conn()
    crs = conn.cursor()
    crs.execute("INSERT INTO requests (screen_name, kind) VALUES (%s, %s);", request)
    conn.commit()


def prioritize_by_last_scrape(screen_names: List[str]) -> List[str]:
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


def prioritize_by_uncollected(screen_names: List[str]) -> List[str]:
    """ Prioritizes by inferring how many tweets have happened since the last scrape for each
        screen name.
    """
    conn = db_conn()
    daily_vol_estimates = {sn: estimate_daily_volume(conn, sn) for sn in screen_names}
    collect_age = {sn: days_since_collect(conn, sn) for sn in screen_names}

    return sorted(screen_names,
                  key=lambda sn: collect_age.get(sn, 100) * daily_vol_estimates.get(sn, 1))


def estimate_daily_volume(conn, screen_name: str) -> float:
    """ Estimate the number of tweets an account gets a day """
    query = """
        WITH last_scrape AS (SELECT max(created_at) AS last FROM requests WHERE screen_name=%(sn)s)
        SELECT count(*) / 3.0 AS daily_tweets FROM tweets, last_scrape
        WHERE tweets.created_at > (last_scrape.last - INTERVAL '3 days')
          AND tweets.data #>> '{user,screen_name}' ILIKE %(sn)s;
    """
    crs = conn.cursor()
    crs.execute(query, {'sn': screen_name.strip('@').lower()})
    return crs.fetchall()[0].daily_tweets


def days_since_collect(conn, screen_name: str) -> float:
    """ Get the number of days since the screen name has been collected """
    return get_all_days_since_collect(conn)[screen_name.strip('@').lower()]


@toolz.memoize
def get_all_days_since_collect(conn) -> Dict[str, float]:
    """ Gets a dict of all days since collect """
    query = """SELECT screen_name, min((now() at time zone 'utc') - created_at) 
               FROM requests GROUP BY screen_name;"""
    crs = conn.cursor()
    crs.execute(query)
    return dict(crs.fetchall())

