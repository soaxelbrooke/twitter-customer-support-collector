# Twitter Customer Support Collector

Collects tweets from companies providing customer support on Twitter.

## Setup

The project uses Postgres, so you'll need to create a database for it:

```bash
$ createdb twitter_cs
$ psql twitter_cs -f create.sql
```

You'll need to provide your consumer and access keys and tokens.  This can be done by setting env variables, or by providing them in the .env file.

```bash
$ cp example.env .env
$ vim .env # and then insert your keys/secrets
```

## Run

Running the script once will scrape the provided list of


