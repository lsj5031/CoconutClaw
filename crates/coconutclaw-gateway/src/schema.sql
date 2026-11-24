CREATE TABLE IF NOT EXISTS inbox (
    sequence INTEGER PRIMARY KEY,
    update_id INTEGER NOT NULL UNIQUE,
    disposition TEXT NOT NULL CHECK (disposition IN ('task', 'ignored'))
) STRICT;
CREATE TABLE IF NOT EXISTS tasks (
    sequence INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    chat_id INTEGER NOT NULL,
    thread_id INTEGER,
    prompt TEXT NOT NULL,
    pin TEXT,
    current_attempt INTEGER REFERENCES attempts(id),
    cancelled INTEGER NOT NULL DEFAULT 0 CHECK (cancelled IN (0, 1))
) STRICT;
CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY,
    task TEXT NOT NULL REFERENCES tasks(id),
    device TEXT NOT NULL,
    lease_until INTEGER NOT NULL,
    start_until INTEGER,
    phase TEXT NOT NULL CHECK (phase IN (
        'claimed', 'expired', 'authorized', 'started', 'unknown',
        'cancelled_before_start', 'cancelled_after_exit', 'completed'
    )),
    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
    started_evidence INTEGER NOT NULL DEFAULT 0 CHECK (started_evidence IN (0, 1))
) STRICT;
CREATE TABLE IF NOT EXISTS agent_sequences (
    device TEXT PRIMARY KEY,
    sequence INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS outbox (
    task TEXT PRIMARY KEY REFERENCES tasks(id),
    text TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('pending', 'in_flight', 'delivered', 'unknown', 'failed')),
    sends INTEGER NOT NULL DEFAULT 0 CHECK (sends BETWEEN 0 AND 2),
    ready_at INTEGER NOT NULL DEFAULT 0
) STRICT;
CREATE TABLE IF NOT EXISTS schedule (
    slot INTEGER PRIMARY KEY CHECK (slot = 1),
    version INTEGER NOT NULL,
    first_at INTEGER NOT NULL,
    next_at INTEGER NOT NULL,
    interval_ms INTEGER NOT NULL,
    run_count INTEGER NOT NULL CHECK (run_count BETWEEN 1 AND 3),
    remaining INTEGER NOT NULL CHECK (remaining BETWEEN 0 AND 3),
    chat_id INTEGER NOT NULL,
    thread_id INTEGER,
    prompt TEXT NOT NULL,
    pin TEXT
) STRICT;
CREATE TABLE IF NOT EXISTS occurrences (
    scheduled_at INTEGER PRIMARY KEY,
    version INTEGER NOT NULL,
    task TEXT NOT NULL UNIQUE REFERENCES tasks(id)
) STRICT;
