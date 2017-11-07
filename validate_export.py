""" Validates exported dataset. """
import csv
import logging
import os
import sys
from collections import namedtuple

import toolz


def validate_export(export_path: str):
    logging.info(f'Validating export at "{export_path}"...')
    logging.info("Reading exported dataset...")
    with open(export_path) as infile:
        reader = csv.reader(infile)
        Tweet = namedtuple('Tweet', next(reader))
        tweets = {int(line[0]): Tweet(*line) for line in reader if len(line) > 0}

    logging.info(f"Read {len(tweets)} tweets.")

    all_author_ids = set(t.author_id for t in tweets.values())
    logging.info(f"Found {len(all_author_ids)} different authors.")

    orphans = [t for t in tweets.values()
               if len(t.response_tweet_id) == 0 and len(t.in_response_to_tweet_id) == 0]
    logging.info(f"Found {len(orphans)} orphan tweets.")

    first_requests = [
        t for t in tweets.values() if t.in_response_to_tweet_id == ''
    ]
    logging.info(f"Found {len(first_requests)} conversation starts.")

    replies = list(toolz.concat(
        [tweets[int(tid)] for tid in t.response_tweet_id.split(',') if int(tid) in tweets]
        for t in first_requests if len(t.response_tweet_id) != ''
    ))

    non_cs_replies = [t for t in replies if not t.inbound]
    logging.info(f"Found {len(non_cs_replies)} non-inbound response tweets out of {len(replies)}.")


if __name__ == '__main__':
    export_path = 'twcs.csv' if len(sys.argv) < 2 else sys.argv[1]
    logging.basicConfig(
        format='%(levelname)s:%(asctime)s.%(msecs)03d [%(threadName)s] - %(message)s',
        datefmt='%Y-%m-%d,%H:%M:%S',
        level=getattr(logging, os.environ.get('LOG_LEVEL', 'INFO')))
    validate_export(export_path)
