""" Tools for fetching tweets using the API """
from datetime import datetime

import os
from typing import Optional, List, NamedTuple, Tuple

from twitter import Api, Status

MAX_FETCH_COUNT = 100

ApiRequest = NamedTuple('ApiRequest', [
    ('screen_name', str),
    ('request_kind', str)
])


def get_api() -> Api:
    """ Memoized constructor for API that pulls secrets from env """
    return Api(consumer_key=os.environ['TWITTER_CONSUMER_KEY'],
               consumer_secret=os.environ['TWITTER_CONSUMER_SECRET'],
               access_token_key=os.environ['TWITTER_ACCESS_TOKEN'],
               access_token_secret=os.environ['TWITTER_ACCESS_SECRET'])


def fetch_tweets_at_user(screen_name: str, since: datetime=datetime(1999, 1, 1)
                         ) -> Tuple[List[Status], ApiRequest]:
    """ Fetches the most recent 100 tweets at the provided screen name """
    api = get_api()
    return api.GetSearch(term='@{}'.format(screen_name), count=MAX_FETCH_COUNT,
                         since=since.strftime('%Y-%m-%d')), ApiRequest(screen_name, 'get_ats')


def fetch_replies_from_user(screen_name: str, since_id: Optional[str]=None
                            ) -> Tuple[List[Status], ApiRequest]:
    """ Fetches the most recent 100 replies from the provided screen name """
    api = get_api()
    return api.GetUserTimeline(screen_name=screen_name, exclude_replies=False, since_id=since_id,
                               count=MAX_FETCH_COUNT), ApiRequest(screen_name, 'get_replies')
