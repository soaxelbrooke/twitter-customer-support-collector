import logging
import os
import traceback

import toolz
from dotenv import load_dotenv
from twitter import TwitterError

import fetch
import db


# Rate limit info for app-based access per 15 minute period:
# search/tweets - 450
# statuses/lookup - 300
API_LIMIT = int(os.environ['SCREEN_NAMES_LIMIT'])


def main():
    """ Run the collector """
    load_dotenv('.env')
    logging.basicConfig(
        format='%(levelname)s:%(asctime)s.%(msecs)03d [%(threadName)s] - %(message)s',
        datefmt='%Y-%m-%d,%H:%M:%S',
        level=getattr(logging, os.environ.get('LOG_LEVEL', 'INFO')))

    logging.info("Starting twitter scrape...")
    monitored_screen_names = [sn.strip('@').lower()
                              for sn in os.environ['MONITORED_SCREEN_NAMES'].split(',')]
    screen_names_to_collect = db.prioritize_by_uncollected(monitored_screen_names)[:API_LIMIT]
    logging.info(f'Collecting the following screen names: {", ".join(screen_names_to_collect)}')

    for screen_name in screen_names_to_collect:
        clean_sn = screen_name.strip('@').lower()
        last_scraped_at = db.last_scraped_at(clean_sn)
        logging.info(f"Collecting tweets for {screen_name}...")

        try:
            tweets, request = fetch.fetch_replies_from_user(clean_sn, )
            logging.info(f"Removing old tweets from {len(tweets)} fetched...")
            old_tweet_ids = set(db.get_existing_tweet_ids([str(t.id) for t in tweets]))
            new_tweets = [t for t in tweets if str(t.id) not in old_tweet_ids]
            logging.info("Saving replies request...")
            db.save_request(request)
            logging.info(f"Saving {len(new_tweets)} tweets...")
            db.save_tweets(new_tweets)
            logging.info("Finished saving replies.")
        except TwitterError:
            logging.error(f"Failed to fetch replies from {screen_name}:")
            traceback.print_exc()

        try:
            tweets, request = fetch.fetch_tweets_at_user(clean_sn, since=last_scraped_at)
            logging.info(f"Removing old tweets from {len(tweets)} fetched...")
            old_tweet_ids = set(db.get_existing_tweet_ids([str(t.id) for t in tweets]))
            new_tweets = [t for t in tweets if str(t.id) not in old_tweet_ids]
            logging.info("Saving ats request...")
            db.save_request(request)
            logging.info(f"Saving {len(new_tweets)} tweets...")
            db.save_tweets(new_tweets)
        except TwitterError:
            logging.error(f"Failed to fetch tweets at {screen_name}:")
            traceback.print_exc()

        logging.info(f"Finished collection for {screen_name}.")

    logging.info("Fetching orphaned tweets...")
    orphaned_tweet_ids = set(db.get_orphaned_tweets())
    orphan_batches = [*toolz.take(250, toolz.partition_all(100, orphaned_tweet_ids))]

    for tweet_ids in orphan_batches:
        logging.info(f"Fetching {len(tweet_ids)} orphans from twitter...")
        tweets = fetch.fetch_tweets_by_id(tweet_ids)
        inaccessible_tweets = {*map(int, tweet_ids)}.difference({int(t.id) for t in tweets})
        logging.info(f"Saving {len(inaccessible_tweets)} inaccessible tweets...")
        db.save_inaccessible_tweet_ids(list(inaccessible_tweets))
        logging.info(f"Saving {len(tweets)} tweets...")
        db.save_tweets(tweets)

    truncated_tweets = db.get_truncated_tweets()
    logging.info(f"Found {len(truncated_tweets)} truncated tweets that need re-fetching.")
    remaining_fetches = 250 - len(orphan_batches)
    truncate_batches = [*toolz.take(remaining_fetches, toolz.partition_all(100, truncated_tweets))]
    try:
        for tweet_ids in truncate_batches:
            logging.info(f"Fetching {len(tweet_ids)} truncated tweets...")
            tweets = fetch.fetch_tweets_by_id(tweet_ids)
            inaccessible_tweets = {*map(int, tweet_ids)}.difference({int(t.id) for t in tweets})
            if inaccessible_tweets:
                logging.info(f"Deleting {len(inaccessible_tweets)} missing tweets...")
                db.delete_tweets(list(map(str, inaccessible_tweets)))

            if tweets:
                logging.info(f"Saving {len(tweets)} tweets...")
                db.save_tweets(tweets, overwrite=True)
            else:
                logging.info('No tweetse to save.')

    except TwitterError:
        logging.error(f"Failed to fetch truncated tweets.")
        traceback.print_exc()

    logging.info("Done!")


if __name__ == '__main__':
    main()
