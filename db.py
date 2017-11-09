import logging
from datetime import datetime
from typing import List, Dict

import psycopg2
import toolz
from psycopg2.extras import NamedTupleConnection, execute_values
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


def user_to_record(user: User) -> tuple:
    """ Converts a user to a record that can be saved in postgres """
    return str(user.id), user.AsJsonString().replace('\u0000', '')


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
    json_string = tweet.AsJsonString().replace('\u0000', '')
    return str(tweet.id), datetime.fromtimestamp(tweet.created_at_in_seconds), json_string


def save_tweets(tweets: List[Status], overwrite=False):
    """ Saves a list of tweets to postgres """
    save_users([t.user for t in tweets])
    unique_tweets = [*toolz.unique(tweets, key=lambda t: t.id)]
    conn = db_conn()
    crs = conn.cursor()

    if overwrite:
        conflict_clause = "(status_id) DO UPDATE SET data = EXCLUDED.data"
    else:
        conflict_clause = "DO NOTHING"

    execute_values(crs, f"""INSERT INTO tweets (status_id, created_at, data)
                            VALUES %s ON CONFLICT {conflict_clause};""",
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
    logging.info(f"Prioritizing {len(screen_names)} screen names for scrape...")
    conn = db_conn()
    daily_vol_estimates = {sn: estimate_daily_volume(conn, sn) for sn in screen_names}
    logging.info("Fetching days since collection for each app...")
    collect_age = {sn: days_since_collect(conn, sn) for sn in screen_names}

    inferred_missing = {sn: collect_age.get(sn, 100) * daily_vol_estimates.get(sn, 1)
                        for sn in screen_names}

    for sn, missing in sorted(inferred_missing.items(), key=lambda p: -p[1]):
        logging.info(f"{sn} missing: {missing}")

    return sorted(screen_names, key=lambda sn: -inferred_missing[sn])


def estimate_daily_volume(conn, screen_name: str) -> float:
    """ Estimate the number of tweets an account gets a day """
    logging.info(f"Estimating daily volume for {screen_name}...")
    query = """
        WITH last_scrape AS (SELECT max(created_at) AS last FROM requests WHERE screen_name=%(sn)s)
        SELECT count(*) / 3.0 AS daily_tweets FROM tweets, last_scrape
        WHERE tweets.created_at > (last_scrape.last - INTERVAL '3 days')
          AND tweets.data #>> '{user,screen_name}' ILIKE %(sn)s;
    """
    crs = conn.cursor()
    crs.execute(query, {'sn': screen_name.strip('@').lower()})
    return max([float(crs.fetchall()[0].daily_tweets), 1.0])


def days_since_collect(conn, screen_name: str) -> float:
    """ Get the number of days since the screen name has been collected """
    datetime_since = get_all_days_since_collect(conn).get(screen_name.strip('@').lower())
    return datetime_since.total_seconds() / 86400.0 if datetime_since is not None else 100.0


@toolz.memoize
def get_all_days_since_collect(conn) -> Dict[str, float]:
    """ Gets a dict of all days since collect """
    query = """SELECT screen_name, min((now() at time zone 'utc') - created_at) 
               FROM requests GROUP BY screen_name;"""
    crs = conn.cursor()
    crs.execute(query)
    return dict(crs.fetchall())


def get_orphaned_tweets() -> List[int]:
    """ Finds orphaned tweet IDs.  Orphans are tweets we don't have the in-reply-to tweet yet. """
    no_request_query = """
        SELECT replies.data->>'in_reply_to_status_id' AS reply_to_id
        FROM tweets replies LEFT JOIN tweets requests
          ON replies.data->>'in_reply_to_status_id'=requests.data->>'id'
        LEFT JOIN inaccessible_tweets
          ON  replies.data->>'in_reply_to_status_id'::TEXT=inaccessible_tweets.status_id
        WHERE replies.data->>'in_reply_to_status_id' IS NOT NULL 
          AND requests.data IS NULL
          AND inaccessible_tweets.status_id IS NULL
        LIMIT 25000;
    """

    conn = db_conn()
    crs = conn.cursor()
    crs.execute(no_request_query)
    return [row.reply_to_id for row in crs.fetchall()]


def save_inaccessible_tweet_ids(tweet_ids: List[str]):
    """ Insert inaccessible tweet IDs into postgres """
    conn = db_conn()
    crs = conn.cursor()
    execute_values(crs, "INSERT INTO inaccessible_tweets (status_id) VALUES %s",
                   [(status_id, ) for status_id in tweet_ids])


def get_truncated_tweets() -> List[int]:
    """ Finds a list of tweets that have been truncated """
    conn = db_conn()
    crs = conn.cursor()
    crs.execute("""SELECT status_id FROM tweets WHERE CAST(data ->> 'truncated' AS boolean)
                   OR data ->> 'text' ILIKE '%… https://t.co/__________';""")
    return [row[0] for row in crs]


def delete_tweets(ids: List[str]):
    """ Deletes tweets for these tweet status IDs """
    conn = db_conn()
    crs = conn.cursor()

    array_ids = ', '.join(f"'{_id}'" for _id in ids)
    crs.execute(f"DELETE FROM tweets WHERE status_id = ANY(ARRAY[{array_ids}]);")

    conn.commit()
