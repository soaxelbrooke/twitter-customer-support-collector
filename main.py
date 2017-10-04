import logging
import os

from dotenv import load_dotenv

import fetch
import db


def main():
    """ Run the collector """
    load_dotenv('.env')
    logging.basicConfig(
        format='%(levelname)s:%(asctime)s.%(msecs)03d [%(threadName)s] - %(message)s',
        datefmt='%Y-%m-%d,%H:%M:%S',
        level=getattr(logging, os.environ.get('LOG_LEVEL', 'INFO')))

    monitored_screen_names = os.environ['MONITORED_SCREEN_NAMES'].split(',')

    for screen_name in db.prioritize_screen_names(monitored_screen_names):
        clean_sn = screen_name.strip('@').lower()
        logging.info(f"Collecting tweets for {screen_name}...")

        tweets, request = fetch.fetch_replies_from_user(clean_sn)
        logging.info("Saving replies request...")
        db.save_request(request)
        logging.info(f"Saving {len(tweets)} tweets...")
        db.save_tweets(tweets)

        tweets, request = fetch.fetch_tweets_at_user(clean_sn)
        logging.info("Saving ats request...")
        db.save_request(request)
        logging.info(f"Saving {len(tweets)} tweets...")
        db.save_tweets(tweets)

        logging.info(f"Finished collection for {screen_name}.")


if __name__ == '__main__':
    main()
