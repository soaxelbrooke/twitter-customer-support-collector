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


def export_to(fileio):
    """ Writes dataset to provided file path """
    conn = psycopg2.connect(dbname='twitter_cs')
    crs = conn.cursor()
    crs.execute(EXPORT_QUERY)

    screen_name_to_id = {}
    replies = defaultdict(list)
    replies_to = defaultdict(int)
    unseen_screen_names = defaultdict(lambda: len(unseen_screen_names))
    tweet_ids = defaultdict(lambda : len(tweet_ids))
    user_ids = defaultdict(lambda : len(user_ids))
    header = ['Tweet ID', 'Author ID', 'Inbound', 'Created At', 'Text', 'Response Tweet ID']
    sn_re = re.compile('(\W@|^@)([a-zA-Z0-9_]+)')
    writer = csv.writer(fileio)
    writer.writerow(header)

    rows = list(crs)

    for row in rows:
        # Construct screen name to id mapping and add forward links to replies
        screen_name_to_id[row[2].lower()] = row[1]
        if row[5]:
            replies[row[0]].append(row[5])
            replies_to[row[5]] = row[0]

    for row in rows:
        for prefix, sn in sn_re.findall(row[4]):
            _sn = sn.lower()
            if _sn not in screen_name_to_id:
                screen_name_to_id[_sn] = unseen_screen_names[_sn]
                user_ids[unseen_screen_names[_sn]]

    def replace_sn(sn):
        _sn = sn.group(2).lower()
        if _sn.startswith('__') and _sn.endswith('__'):
            return sn.group(1) + sn.group(2)
        return sn.group(1) + str(user_ids[screen_name_to_id[_sn]])

    sn_sanitize = lambda text: sn_re.sub(replace_sn, text)
    email_re = re.compile(commonregex.email)
    email_sanitize = lambda text: email_re.sub('__email__', text)
    cc_re = re.compile(commonregex.credit_card)
    cc_sanitize = lambda text: cc_re.sub('__creditcard__', text)
    btc_re = re.compile(commonregex.btc_address)
    btc_sanitize = lambda text: btc_re.sub('__btc_wallet__', text)
    sanitize = toolz.compose(cc_sanitize, btc_sanitize, sn_sanitize, email_sanitize)

    for row in rows:
        if row[5] not in replies_to and len(replies[row[0]]) == 0:
            # Skip tweets with no replies that don't reply to others
            continue

        tweet_id = tweet_ids[row[0]]
        author_id = user_ids[row[1]]
        inbound = row[2].lower() not in CUSTOMER_SUPPORT_SNS
        created_at = row[3]
        text = sanitize(row[4])
        response_tweet_ids = ','.join([str(tweet_ids[reply]) for reply in replies[row[0]]])
        writer.writerow([tweet_id, author_id, inbound, created_at, text, response_tweet_ids])


if __name__ == '__main__':
    outpath = os.environ.get('OUTFILE')

    if outpath:
        with open(outpath, 'w') as outfile:
            export_to(outfile)
    else:
        export_to(sys.stdout)


