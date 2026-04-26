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
  duration_ms INTEGER,
  task_run_id INTEGER,
  side_effects_applied INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_turns_update_id_unique ON turns(update_id);

CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  source TEXT NOT NULL,
  content TEXT NOT NULL,
  done INTEGER NOT NULL DEFAULT 0,
  turn_id INTEGER,
  append_index INTEGER,
  managed_file INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS memory_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  source TEXT NOT NULL,
  content TEXT NOT NULL,
  turn_id INTEGER NOT NULL,
  append_index INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_entries_turn_append_unique ON memory_entries(turn_id, append_index);

CREATE TABLE IF NOT EXISTS scheduled_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  source TEXT NOT NULL,
  prompt TEXT NOT NULL,
  schedule_time TEXT NOT NULL,
  recurring INTEGER NOT NULL DEFAULT 1,
  last_run_ts TEXT,
  done INTEGER NOT NULL DEFAULT 0,
  pending_output TEXT,
  delivery_state TEXT,
  origin_session TEXT,
  delivery_target TEXT
);

CREATE TABLE IF NOT EXISTS task_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  channel TEXT NOT NULL,
  source_chat_id TEXT,
  source_user_id TEXT,
  update_id TEXT,
  prompt TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  cancel_requested_at TEXT,
  progress_message_id TEXT,
  last_progress TEXT,
  error_summary TEXT,
  result_summary TEXT,
  scheduled_task_id INTEGER
);
CREATE INDEX IF NOT EXISTS idx_task_runs_session_status ON task_runs(session_id, status, id DESC);
CREATE INDEX IF NOT EXISTS idx_task_runs_status ON task_runs(status, id DESC);

CREATE TABLE IF NOT EXISTS approvals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_run_id INTEGER NOT NULL,
  session_id TEXT NOT NULL,
  channel TEXT NOT NULL,
  source_user_id TEXT,
  channel_id TEXT,
  thread_ts TEXT,
  prompt_text TEXT NOT NULL,
  status TEXT NOT NULL,
  request_message_ts TEXT,
  resume_payload TEXT NOT NULL,
  created_at TEXT NOT NULL,
  resolved_at TEXT,
  resolved_by_user_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_approvals_task_status ON approvals(task_run_id, status, id DESC);

INSERT OR IGNORE INTO kv(key, value) VALUES ('last_update_id', '0');
