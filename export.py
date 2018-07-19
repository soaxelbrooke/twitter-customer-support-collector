""" Exports TWCS dataset """
import csv
import os
import re
import sys
from collections import defaultdict

import commonregex
import psycopg2
import toolz

EXPORT_QUERY = """
    SELECT 
      CAST(status_id AS BIGINT),
      CAST(data #>> '{user,id}' AS BIGINT),
      data #>> '{user,screen_name}', 
      data ->>'created_at',
      data ->> 'text',
      data ->> 'full_text',
      CAST(data ->> 'in_reply_to_status_id' AS BIGINT)
    FROM tweets;
"""

CUSTOMER_SUPPORT_SNS = {
    'nikesupport', 'xboxsupport', 'upshelp', 'comcastcares', 'amazonhelp', 'jetblue', 'americanair',
    'tacobellteam', 'mcdonalds', 'kimpton', 'ihgservice', 'spotifycares', 'hiltonhelp',
    'applesupport', 'microsofthelps', 'googleplaymusic', 'scsupport', 'pandorasupport',
    'hoteltonightcx', 'dunkindonuts', 'jackbox', 'chipotletweets', 'askpanera', 'carlsjr', 'att',
    'tmobilehelp', 'sprintcare', 'verizonsupport', 'boostcare', 'uscellularcares', 'alaskaair',
    'virginamerica', 'virginatlantic', 'delta', 'british_airways', 'southwestair', 'awssupport',
    'twittersupport', 'askplaystation', 'neweggservice', 'dropboxsupport', 'hpsupport',
    'atviassist', 'azuresupport', 'nortonsupport', 'dellcares', 'hulu_support', 'askrobinhood',
    'officesupport', 'arbyscares', 'pearsonsupport', 'yahoocare', 'idea_cares', 'airtel_care',
    'coxhelp', 'kfc_uki_help', 'asurioncares', 'adobecare', 'glocare', 'sizehelpteam',
    'airasiasupport', 'safaricom_care', 'oppocarein', 'bofa_help', 'chasesupport', 'askciti',
    'ask_wellsfargo', 'keybank_help', 'moo', 'centurylinkhelp', 'mediatemplehelp', 'godaddyhelp',
    'postmates_help', 'doordash_help', 'airbnbhelp', 'uber_support', 'asklyft', 'askseagate',
    'ask_spectrum', 'askpaypal', 'asksalesforce', 'askvirginmoney', 'askdsc', 'askpapajohns',
    'askrbc', 'askebay', 'asktigogh', 'vmucare', 'askamex', 'ask_progressive', 'mtnc_care',
    'askvisa', 'tesco', 'sainsburys', 'walmart', 'asktarget', 'morrisons', 'aldiuk', 'argoshelpers',
    'greggsofficial', 'marksandspencer', 'virgintrains', 'nationalrailenq', 'sw_help',
    'londonmidland', 'gwrhelp', 'tfl', 'o2'
}

ANON = True


def export_to(fileio):
    """ Writes dataset to provided file path """
    conn = psycopg2.connect(dbname='twitter_cs')
    crs = conn.cursor()
    crs.execute(EXPORT_QUERY)

    screen_name_to_id = {}
    replies = defaultdict(list)
    unseen_screen_names = defaultdict(lambda: len(unseen_screen_names))
    tweet_ids = defaultdict(lambda : len(tweet_ids))
    tweet_ids[None] = ''
    user_ids = defaultdict(lambda : len(user_ids))
    header = ['tweet_id', 'author_id', 'inbound', 'created_at', 'text', 'response_tweet_id',
              'in_response_to_tweet_id']
    sn_re = re.compile('(\W@|^@)([a-zA-Z0-9_]+)')
    writer = csv.writer(fileio)
    writer.writerow(header)

    rows = list(crs)
    row_dict = {row[0]: row for row in rows}

    for row in rows:
        # Construct screen name to id mapping and add forward links to replies
        if isinstance(row[2], str):
            screen_name_to_id[row[2].lower()] = row[1]
            if row[6]:
                replies[row[6]].append(row[0])

    for row in rows:
        for prefix, sn in sn_re.findall(row[4] or row[5]):
            _sn = sn.lower()
            if _sn not in screen_name_to_id:
                screen_name_to_id[_sn] = unseen_screen_names[_sn]
                user_ids[unseen_screen_names[_sn]]

    def replace_sn(sn):
        _sn = sn.group(2).lower()
        if _sn in CUSTOMER_SUPPORT_SNS or _sn.startswith('__') and _sn.endswith('__'):
            return sn.group(1) + sn.group(2)
        return sn.group(1) + str(user_ids[screen_name_to_id[_sn]])

    sn_sanitize = lambda text: sn_re.sub(replace_sn, text)
    email_re = re.compile(commonregex.email)
    email_sanitize = lambda text: email_re.sub('__email__', text)
    cc_re = re.compile(commonregex.credit_card)
    cc_sanitize = lambda text: cc_re.sub('__credit_card__', text)
    btc_re = re.compile(commonregex.btc_address)
    btc_sanitize = lambda text: btc_re.sub('__btc_wallet__', text)
    sanitize = toolz.compose(cc_sanitize, btc_sanitize, sn_sanitize, email_sanitize)

    def write_row(row: list):
        """ Writes tweet to file if it hasn't been written yet. """
        if row[0] in written_tweet_ids:
            return
        is_company = row[2].lower() in CUSTOMER_SUPPORT_SNS
        tweet_id = tweet_ids[row[0]] if ANON else row[0]
        author_id = (row[2] if is_company else user_ids[row[1]]) if ANON else row[2]
        inbound = row[2].lower() not in CUSTOMER_SUPPORT_SNS
        created_at = row[3]
        text = sanitize(row[4] or row[5]) if ANON else row[4] or row[5]
        response_tweet_ids = ','.join([str(tweet_ids[reply]) for reply in replies[row[0]]]) \
            if ANON else ','.join(map(str, replies[row[0]]))
        respond_to_id = tweet_ids[row[6]] if ANON else row[6]
        writer.writerow([tweet_id, author_id, inbound, created_at, text, response_tweet_ids,
                         respond_to_id])
        written_tweet_ids.add(row[0])

    written_tweet_ids = set()

    def walk_conversations_fowards(row: list):
        """ Walk tweet conversations forward and writes them to file """
        write_row(row)
        for reply in replies[row[0]]:
            if reply not in row_dict:
                continue
            _row = row_dict[reply]
            write_row(_row)
            walk_conversations_fowards(_row)

    def walk_conversations_backwards(row: list):
        """ Walks tweet conversations and writes them to file """
        write_row(row)
        if row[6] and row[6] in row_dict:
            walk_conversations_backwards(row_dict[row[6]])

    for row in rows:
        if row[2].lower() not in CUSTOMER_SUPPORT_SNS or not row[6] or row[6] not in row_dict:
            # Skip non-customer support response tweets to start each conversation
            continue

        walk_conversations_fowards(row)
        walk_conversations_backwards(row)


if __name__ == '__main__':
    outpath = os.environ.get('OUTFILE')

    if outpath:
        with open(outpath, 'w') as outfile:
            export_to(outfile)
    else:
        export_to(sys.stdout)


