import logging
import os

from dotenv import load_dotenv

import fetch


def main():
    """ Run the collector """
    load_dotenv('.env')
    logging.basicConfig(
        format='%(levelname)s:%(asctime)s.%(msecs)03d [%(threadName)s] - %(message)s',
        datefmt='%Y-%m-%d,%H:%M:%S',
        level=getattr(logging, os.environ.get('LOG_LEVEL', 'INFO')))

    monitored_screen_names = [*map(str.strip('@'), os.environ['MONITORED_SCREEN_NAMES'].split(','))]

    for screen_name in monitored_screen_names:
        logging.info(f"Collecting tweets for ${screen_name}...")
        fetch.fetch_replies_from_user(screen_name)


if __name__ == '__main__':
    main()
