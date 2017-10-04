
CREATE TABLE IF NOT EXISTS tweets (
  status_id TEXT PRIMARY KEY,
  created_at TIMESTAMP,
  observed_at TIMESTAMP default current_timestamp,
  data JSONB
);

CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  observed_at TIMESTAMP DEFAULT current_timestamp,
  data JSONB
);
