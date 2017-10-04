
CREATE TABLE IF NOT EXISTS tweets (
  status_id TEXT PRIMARY KEY,
  created_at TIMESTAMP,
  observed_at TIMESTAMP default (now() at time zone 'utc'),
  data JSONB
);

CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  observed_at TIMESTAMP DEFAULT (now() at time zone 'utc'),
  data JSONB
);

CREATE TABLE IF NOT EXISTS requests (
  id BIGSERIAL PRIMARY KEY,
  screen_name TEXT,
  request_kind TEXT,
  created_at TIMESTAMP DEFAULT (now() at time zone 'utc')
);
