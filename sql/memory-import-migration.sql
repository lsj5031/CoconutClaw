-- Preserve existing bounded-memory IDs/history while allowing non-turn origins.
-- Executed inside the memory initialization transaction, before any import.
CREATE TEMP TABLE saved_records AS SELECT * FROM memory_records;
CREATE TEMP TABLE saved_active AS SELECT * FROM memory_active;
CREATE TEMP TABLE saved_history AS SELECT * FROM memory_history;
CREATE TEMP TABLE saved_receipts AS SELECT * FROM memory_command_receipts;
DROP TABLE memory_active;
DROP TABLE memory_search;
DROP TABLE memory_history;
DROP TABLE memory_command_receipts;
DROP TABLE memory_records;
