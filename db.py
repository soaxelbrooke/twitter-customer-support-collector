import logging
from datetime import datetime
from typing import List, Dict
import json

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


def last_scraped_at(screen_name: str) -> datetime:
    """ Get the last time the screen name was scraped from the database. """
    conn = db_conn()
    crs = conn.cursor()
    query = 'SELECT created_at FROM requests WHERE screen_name=%s ORDER BY created_at DESC LIMIT 1'
    crs.execute(query, (screen_name, ))
    results = crs.fetchall()
    if len(results) < 1:
        return datetime(1999, 1, 1)
    else:
        return results[0][0]


def get_existing_tweet_ids(tweet_ids: List[str]) -> List[str]:
    """ Fetches list of tweet IDs that are already in the database """
    conn = db_conn()
    crs = conn.cursor()
    crs.execute('SELECT status_id FROM tweets WHERE status_id=ANY(%s)', (tweet_ids,))
    return [row[0] for row in crs]


def save_users(users: List[User]):
    """ Saves users pulled from tweets """
    query = """INSERT INTO users (user_id, data) VALUES %s ON CONFLICT DO NOTHING;"""
    unique_users = [*toolz.unique(users, key=lambda u: u.id)]
    conn = db_conn()
    try:
        crs = conn.cursor()
        execute_values(crs, query, [*map(user_to_record, unique_users)])
        conn.commit()
    except psycopg2.DataError:
        conn.rollback()
        crs = conn.cursor()
        for user in unique_users:
            try:
                crs.execute(query, user_to_record(user))
                conn.commit()
            except psycopg2.DataError:
                logging.error('Failed to insert user {}, giving up on them.'.format(user))
            


def tweet_to_record(tweet: Status) -> tuple:
    """ Converts a tweet to a record that can be saved in postgres """
    json_string = tweet.AsJsonString().replace('\u0000', '')
    return str(tweet.id), datetime.fromtimestamp(tweet.created_at_in_seconds), json_string


def add_sentiment_to_records(analyzer, records):
    """ Adds sentiment to records before insertion into postgres """
    texts = []
    for tweet_id, created_at, json_string in records:
        tweet = json.loads(json_string)
        texts.append(tweet.get('text') or tweet.get('full_text'))

    sentiments = [*map(float, analyzer.analyze(texts))]

    new_records = []
    for (tweet_id, created_at, json_string), sentiment in zip(records, sentiments):
        tweet = json.loads(json_string)
        tweet['sentiment'] = sentiment
        new_records.append((tweet_id, created_at, json.dumps(tweet)))

    return new_records


def save_tweets(tweets: List[Status], overwrite=False, sentiment_analyzer=None):
    """ Saves a list of tweets to postgres """
    save_users([t.user for t in tweets])
    unique_tweets = [*toolz.unique(tweets, key=lambda t: t.id)]
    conn = db_conn()
    crs = conn.cursor()

    records = [*map(tweet_to_record, unique_tweets)]
    if sentiment_analyzer is not None and len(records) > 0:
        logging.info(f"Calculating sentiment for {len(records)} records...")
        records = add_sentiment_to_records(sentiment_analyzer, records)

    if overwrite:
        conflict_clause = "(status_id) DO UPDATE SET data = EXCLUDED.data"
    else:
        conflict_clause = "DO NOTHING"

    execute_values(crs, f"""INSERT INTO tweets (status_id, created_at, data)
                            VALUES %s ON CONFLICT {conflict_clause};""",
                   records)
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
    query = """
        WITH max_request AS (
          SELECT max(created_at) AS max_created_at FROM requests
        ), daily_counts AS (
          SELECT
            lower(data #>> '{user,screen_name}') AS screen_name,
            EXTRACT(EPOCH FROM max(created_at) - min(created_at)) / 86400.0 AS tweet_period,
            count(*) AS tweet_count
          FROM tweets
          WHERE created_at > now() - INTERVAL '1 week'
          GROUP BY 1
        ), last_scrapes AS (
          SELECT
            screen_name,
            EXTRACT(EPOCH FROM max_created_at - max(created_at)) / 86400.0 AS scrape_age
          FROM requests, max_request
          GROUP BY screen_name, max_created_at
        )
        SELECT
          screen_name,
          abs(scrape_age * tweet_count / (tweet_period + 0.0001)) AS missing_tweets,
          scrape_age,
          tweet_count,
          tweet_period
        FROM daily_counts RIGHT JOIN last_scrapes USING (screen_name)
        ORDER BY 2 DESC;
    """
    crs = conn.cursor()
    crs.execute(query)

    priortized_sns = [row[0] for row in crs]

    found_sns = set(priortized_sns)

    return [sn for sn in screen_names if sn not in found_sns] + priortized_sns


def estimate_daily_volume(conn) -> Dict[str, float]:
    """ Estimate the number of tweets an account gets a day """
    logging.info(f"Estimating daily volume...")
    query = """
        WITH last_scrape AS (
          SELECT screen_name, max(created_at) AS last FROM requests GROUP BY screen_name
        ), sn_tweets AS (
          SELECT data #>> '{user,screen_name}' as sn, created_at FROM tweets 
          ORDER BY created_at DESC LIMIT 200000
        ) SELECT screen_name, count(*) / 3.0 AS daily_tweets 
        FROM last_scrape JOIN sn_tweets ON sn ILIKE screen_name 
        WHERE created_at > last_scrape.last - INTERVAL '3 days' GROUP BY screen_name;  
    """
    crs = conn.cursor()
    crs.execute(query)
    return {sn: daily_count for sn, daily_count in crs}


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
          AND replies.created_at > (now() - interval '1 day')
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
