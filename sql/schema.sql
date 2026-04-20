PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS turns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  chat_id TEXT NOT NULL,
  input_type TEXT NOT NULL,
  user_text TEXT,
  asr_text TEXT,
  provider_raw TEXT NOT NULL,
  telegram_reply TEXT,
  voice_reply TEXT,
  status TEXT NOT NULL,
  update_id TEXT,
  duration_ms INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_turns_update_id_unique ON turns(update_id);

CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  source TEXT NOT NULL,
  content TEXT NOT NULL,
  done INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS scheduled_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  source TEXT NOT NULL,
  prompt TEXT NOT NULL,
  schedule_time TEXT NOT NULL,
  recurring INTEGER NOT NULL DEFAULT 1,
  last_run_ts TEXT,
  done INTEGER NOT NULL DEFAULT 0,
  pending_output TEXT
);

INSERT OR IGNORE INTO kv(key, value) VALUES ('last_update_id', '0');
