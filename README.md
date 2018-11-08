# Twitter Customer Support Collector

Collects tweets from companies providing customer support on Twitter.

Compatibility: Python 3.6 and up.

## Setup

The project uses Postgres, so you'll need to create a database for it:

```bash
$ sudo pg_createcluster -p 5455 11 twcs
$ sudo systemctl daemon-reload
$ sudo systemctl restart postgresql@11-twcs
$ sudo -Hu postgres psql -p 5455 -c "create role stuart superuser login;" && sudo -Hu postgres psql -p 5455 -c "create database twitter_cs with owner stuart;"
$ psql twitter_cs -f create.sql
```

You'll need to provide your consumer and access keys and tokens.  This can be done by setting env variables, or by providing them in the .env file.

```bash
$ cp example.env .env
$ vim .env # and then insert your keys/secrets, change accounts to scrape
```

## Run

Running the script:

```bash
$ PYTHONPATH=$(pwd) python3.6 main.py
```

You should be able to run it every 15 minutes without going over API limits, so running it from Jenkins or cron is a great match.

## Export

The `/copy` command in psql will let you export your scraped data to CSV:

```SQL
twitter_cs=> \copy (SELECT request.created_at AS date, request.data#>>'{user,screen_name}' AS request_screen_name, request.data->>'text' AS request_text, reply.data#>>'{user,screen_name}' AS reply_screen_name, reply.data->>'text' AS reply_text FROM tweets reply INNER JOIN tweets request ON reply.data ->> 'in_reply_to_status_id' = request.status_id LIMIT 10) TO 'twitter_cs.csv' WITH CSV HEADER;
```
