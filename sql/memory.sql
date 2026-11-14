-- Local core-owned memory, no legacy imports or projections.
CREATE TABLE IF NOT EXISTS memory_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope TEXT NOT NULL,
    source_turn INTEGER NOT NULL REFERENCES turns(id),
    proposal_index INTEGER,
    curated INTEGER NOT NULL DEFAULT 0 CHECK(curated IN (0,1)),
    content TEXT NOT NULL CHECK(length(CAST(content AS BLOB)) BETWEEN 1 AND 2048),
    UNIQUE(source_turn, proposal_index)
);
CREATE INDEX IF NOT EXISTS memory_scope ON memory_records(scope, id);
CREATE TABLE IF NOT EXISTS memory_active (
    record_id INTEGER PRIMARY KEY REFERENCES memory_records(id)
);
CREATE VIRTUAL TABLE IF NOT EXISTS memory_search USING fts5(content);
CREATE TRIGGER IF NOT EXISTS memory_index_add AFTER INSERT ON memory_active BEGIN
    INSERT INTO memory_search(rowid, content)
        SELECT id, content FROM memory_records WHERE id = new.record_id;
END;
CREATE TRIGGER IF NOT EXISTS memory_index_remove AFTER DELETE ON memory_active BEGIN
    DELETE FROM memory_search WHERE rowid = old.record_id;
END;
CREATE TABLE IF NOT EXISTS memory_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope TEXT NOT NULL,
    source_turn INTEGER NOT NULL REFERENCES turns(id),
    kind TEXT NOT NULL CHECK(kind IN ('remember', 'promote', 'correct', 'undo')),
    old_record INTEGER REFERENCES memory_records(id),
    new_record INTEGER REFERENCES memory_records(id),
    undo_of INTEGER UNIQUE REFERENCES memory_history(id)
);
CREATE TABLE IF NOT EXISTS memory_command_receipts (
    scope TEXT NOT NULL,
    request_id TEXT NOT NULL,
    reply TEXT NOT NULL,
    PRIMARY KEY(scope, request_id)
);
CREATE TRIGGER IF NOT EXISTS memory_record_immutable_update BEFORE UPDATE ON memory_records BEGIN
    SELECT RAISE(ABORT, 'memory records are immutable');
END;
CREATE TRIGGER IF NOT EXISTS memory_record_immutable_delete BEFORE DELETE ON memory_records BEGIN
    SELECT RAISE(ABORT, 'memory records are immutable');
END;
CREATE TRIGGER IF NOT EXISTS memory_history_immutable_update BEFORE UPDATE ON memory_history BEGIN
    SELECT RAISE(ABORT, 'memory history is immutable');
END;
CREATE TRIGGER IF NOT EXISTS memory_history_immutable_delete BEFORE DELETE ON memory_history BEGIN
    SELECT RAISE(ABORT, 'memory history is immutable');
END;
