
CREATE TABLE IF NOT EXISTS tweets (
  status_id TEXT PRIMARY KEY,
  created_at TIMESTAMP,
  observed_at TIMESTAMP default (now() at time zone 'utc'),
  data JSONB
);

CREATE INDEX IF NOT EXISTS tweet_created_at ON tweets (created_at);

CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  observed_at TIMESTAMP DEFAULT (now() at time zone 'utc'),
  data JSONB
);

CREATE TABLE IF NOT EXISTS requests (
  id BIGSERIAL PRIMARY KEY,
  screen_name TEXT,
  kind TEXT,
  created_at TIMESTAMP DEFAULT (now() at time zone 'utc')
);

CREATE INDEX IF NOT EXISTS request_created_at ON requests (created_at);
CREATE INDEX IF NOT EXISTS request_kind ON requests (kind);
CREATE INDEX IF NOT EXISTS request_kind_created_at ON requests (kind, created_at);
