-- Opaque Telegram controls are scoped capabilities, never provider instructions.
CREATE TABLE IF NOT EXISTS telegram_ui_observations (seq INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS telegram_ui_controls (
    token TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    navigation_only INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    consumed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS telegram_ui_inputs (
    session_id TEXT PRIMARY KEY,
    actor TEXT NOT NULL,
    token TEXT NOT NULL,
    kind TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS telegram_ui_controls_expiry ON telegram_ui_controls(expires_at);
CREATE INDEX IF NOT EXISTS telegram_ui_inputs_expiry ON telegram_ui_inputs(expires_at);
-- Remote ingress remains gated by the existing verified transport adapter.
-- These are exact source-run bindings/custody, never new notification tasks.
CREATE TABLE IF NOT EXISTS runtime_remote_runs (
    task_run_id INTEGER PRIMARY KEY REFERENCES task_runs(id),
    origin TEXT NOT NULL,
    assignment BLOB NOT NULL,
    session_id TEXT NOT NULL,
    request TEXT NOT NULL,
    UNIQUE(origin, assignment)
);
CREATE TABLE IF NOT EXISTS runtime_remote_results (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    task_run_id INTEGER NOT NULL UNIQUE REFERENCES runtime_remote_runs(task_run_id),
    event_key TEXT NOT NULL UNIQUE REFERENCES runtime_inbox(event_key),
    start BLOB NOT NULL,
    result BLOB NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('completed','failed','cancelled')),
    uncertain INTEGER NOT NULL CHECK(uncertain IN (0,1)),
    output TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','applied','unknown'))
);
-- Runtime request/result outbox. Legacy tables and columns remain untouched.
CREATE TABLE IF NOT EXISTS task_payloads (
    task_run_id INTEGER PRIMARY KEY REFERENCES task_runs(id),
    request TEXT NOT NULL,
    output TEXT,
    delivery_state TEXT NOT NULL DEFAULT '[]',
    delivered INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS legacy_admission_freeze (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1)
);

CREATE TABLE IF NOT EXISTS turns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  chat_id TEXT NOT NULL,
  telegram_chat_id TEXT,
  telegram_message_thread_id TEXT,
  input_type TEXT NOT NULL,
  user_text TEXT,
  asr_text TEXT,
  provider_raw TEXT NOT NULL,
  admitted_output TEXT,
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
  source_thread_id TEXT,
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

-- New runtime events/schedules never activate historical scheduled_tasks.
CREATE TABLE IF NOT EXISTS runtime_inbox (
  event_key TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK(kind IN ('request','schedule','approval','result')),
  session_id TEXT NOT NULL,
  payload TEXT NOT NULL,
  task_run_id INTEGER NOT NULL REFERENCES task_runs(id),
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS runtime_inbox_session ON runtime_inbox(session_id, task_run_id);
CREATE TABLE IF NOT EXISTS runtime_schedules (
  id INTEGER PRIMARY KEY,
  session_id TEXT NOT NULL,
  creation_key TEXT NOT NULL,
  creation_spec TEXT NOT NULL,
  request TEXT NOT NULL,
  timing TEXT NOT NULL,
  revision INTEGER NOT NULL DEFAULT 1,
  state TEXT NOT NULL CHECK(state IN ('active','paused','cancelled','done')),
  next_due INTEGER,
  UNIQUE(session_id, creation_key)
);
CREATE TABLE IF NOT EXISTS schedule_occurrences (
  schedule_id INTEGER NOT NULL REFERENCES runtime_schedules(id),
  revision INTEGER NOT NULL,
  due INTEGER NOT NULL,
  event_key TEXT NOT NULL UNIQUE REFERENCES runtime_inbox(event_key),
  PRIMARY KEY(schedule_id, revision, due)
);
-- Include approval continuation runs in the same no-overlap boundary, without
-- overloading the legacy task_runs.scheduled_task_id foreign identity.
CREATE TABLE IF NOT EXISTS runtime_schedule_runs (
  schedule_id INTEGER NOT NULL REFERENCES runtime_schedules(id),
  task_run_id INTEGER NOT NULL UNIQUE REFERENCES task_runs(id),
  PRIMARY KEY(schedule_id, task_run_id)
);

-- Raw transport admission precedes commands, progress I/O and model admission.
-- A runtime effect interrupted in 'applying' is Unknown, never a retry grant.
CREATE TABLE IF NOT EXISTS telegram_ingress (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  update_id TEXT NOT NULL UNIQUE,
  payload TEXT NOT NULL,
  session_id TEXT,
  state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','applying','done','unknown')),
  output TEXT,
  route TEXT,
  progress_message_id TEXT,
  progress_state TEXT NOT NULL DEFAULT 'none' CHECK(progress_state IN ('none','sending','sent','unknown')),
  ack_phase TEXT NOT NULL DEFAULT 'ready' CHECK(ack_phase IN ('ready','sent','unknown')),
  delivery_state TEXT NOT NULL DEFAULT '[]',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS telegram_ingress_order ON telegram_ingress(session_id,state,seq);

-- Only the authenticated parser grants this SQL-only control a bypass of older
-- Unknown custody. Pending/applying owners still serialize the conversation.
CREATE TABLE IF NOT EXISTS telegram_reconciliation_controls(
  seq INTEGER PRIMARY KEY REFERENCES telegram_ingress(seq)
);
CREATE TABLE IF NOT EXISTS runtime_reconciliation_offers(
  token TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  actor TEXT NOT NULL,
  target_kind TEXT NOT NULL CHECK(target_kind IN ('task','ack')),
  target_id INTEGER NOT NULL CHECK(target_id>0),
  snapshot TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_reconciliation_audit(
  token TEXT PRIMARY KEY REFERENCES runtime_reconciliation_offers(token),
  cause_seq INTEGER NOT NULL UNIQUE REFERENCES telegram_ingress(seq),
  evidence TEXT NOT NULL CHECK(evidence='durable-text-ack'),
  applied_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS reconciliation_offer_immutable_update BEFORE UPDATE ON runtime_reconciliation_offers
BEGIN SELECT RAISE(ABORT,'immutable reconciliation offer'); END;
CREATE TRIGGER IF NOT EXISTS reconciliation_offer_immutable_delete BEFORE DELETE ON runtime_reconciliation_offers
BEGIN SELECT RAISE(ABORT,'immutable reconciliation offer'); END;
CREATE TRIGGER IF NOT EXISTS reconciliation_audit_immutable_update BEFORE UPDATE ON runtime_reconciliation_audit
BEGIN SELECT RAISE(ABORT,'immutable reconciliation audit'); END;
CREATE TRIGGER IF NOT EXISTS reconciliation_audit_immutable_delete BEFORE DELETE ON runtime_reconciliation_audit
BEGIN SELECT RAISE(ABORT,'immutable reconciliation audit'); END;

-- Logical work is distinct from its physical execution/continuation records.
CREATE TABLE IF NOT EXISTS runtime_tasks(
  task_id INTEGER PRIMARY KEY REFERENCES task_runs(id),
  session_id TEXT NOT NULL,
  actor TEXT,
  channel TEXT NOT NULL,
  current_run_id INTEGER NOT NULL UNIQUE REFERENCES task_runs(id),
  parent_task_id INTEGER REFERENCES runtime_tasks(task_id) CHECK(parent_task_id!=task_id)
);
CREATE TABLE IF NOT EXISTS runtime_attempts(
  run_id INTEGER PRIMARY KEY REFERENCES task_runs(id),
  task_id INTEGER NOT NULL REFERENCES runtime_tasks(task_id),
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  parent_run_id INTEGER UNIQUE REFERENCES task_runs(id),
  cause_event_key TEXT UNIQUE REFERENCES runtime_inbox(event_key),
  UNIQUE(task_id,ordinal),
  CHECK((ordinal=1)=(parent_run_id IS NULL))
);
CREATE TABLE IF NOT EXISTS runtime_waits(
  run_id INTEGER PRIMARY KEY REFERENCES runtime_attempts(run_id),
  kind TEXT NOT NULL CHECK(kind IN ('approval','dependency')),
  expected_id INTEGER NOT NULL,
  session_id TEXT NOT NULL,
  actor TEXT,
  resume_payload TEXT NOT NULL,
  run_prompt TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'waiting' CHECK(state IN ('waiting','resumed','cancelled','unknown')),
  resumed_run_id INTEGER UNIQUE REFERENCES task_runs(id),
  satisfied_by_run_id INTEGER REFERENCES task_runs(id),
  completion_output TEXT,
  CHECK((state='resumed')=(resumed_run_id IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS runtime_waits_ready ON runtime_waits(kind,state,run_id);

-- Owner-authored instructions/preferences are not factual memory or model
-- proposals. The raw authenticated control is their immutable provenance.
CREATE TABLE IF NOT EXISTS runtime_preferences(
  id INTEGER PRIMARY KEY,
  session_id TEXT NOT NULL,
  actor TEXT NOT NULL,
  name TEXT NOT NULL,
  value TEXT,
  cause_seq INTEGER NOT NULL UNIQUE REFERENCES telegram_ingress(seq),
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS runtime_preferences_scope ON runtime_preferences(session_id,name,id);
CREATE TRIGGER IF NOT EXISTS preference_immutable_update BEFORE UPDATE ON runtime_preferences
BEGIN SELECT RAISE(ABORT,'immutable owner preference'); END;
CREATE TRIGGER IF NOT EXISTS preference_immutable_delete BEFORE DELETE ON runtime_preferences
BEGIN SELECT RAISE(ABORT,'immutable owner preference'); END;

-- Projection plus immutable decisions, on the existing runtime database.
CREATE TABLE IF NOT EXISTS runtime_task_blockers(
  task_id INTEGER PRIMARY KEY REFERENCES runtime_tasks(task_id),
  identity TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK(generation>0)
);
CREATE TABLE IF NOT EXISTS runtime_notifications(
  id INTEGER PRIMARY KEY,
  task_id INTEGER NOT NULL REFERENCES runtime_tasks(task_id),
  run_id INTEGER NOT NULL REFERENCES task_runs(id),
  generation INTEGER NOT NULL,
  session_id TEXT NOT NULL,
  actor TEXT,
  identity TEXT NOT NULL,
  reason TEXT NOT NULL,
  target TEXT NOT NULL,
  output TEXT NOT NULL,
  provenance TEXT NOT NULL CHECK(provenance IN ('initial-outcome','legacy-baseline','changed-blocker')),
  created_at TEXT NOT NULL,
  disposition TEXT NOT NULL CHECK(disposition IN ('linked','pending','sent','unknown','superseded','suspended')),
  delivery_state TEXT NOT NULL DEFAULT '[]',
  UNIQUE(task_id,generation)
);
CREATE TRIGGER IF NOT EXISTS notification_immutable_update BEFORE UPDATE ON runtime_notifications
WHEN NEW.id!=OLD.id OR NEW.task_id!=OLD.task_id OR NEW.run_id!=OLD.run_id
 OR NEW.generation!=OLD.generation OR NEW.session_id!=OLD.session_id
 OR NEW.actor IS NOT OLD.actor OR NEW.identity!=OLD.identity OR NEW.reason!=OLD.reason
 OR NEW.target!=OLD.target OR NEW.output!=OLD.output OR NEW.provenance!=OLD.provenance
 OR NEW.created_at!=OLD.created_at
BEGIN SELECT RAISE(ABORT,'immutable notification decision'); END;
CREATE TRIGGER IF NOT EXISTS notification_immutable_delete BEFORE DELETE ON runtime_notifications
BEGIN SELECT RAISE(ABORT,'immutable notification decision'); END;
