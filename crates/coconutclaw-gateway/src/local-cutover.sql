CREATE TABLE IF NOT EXISTS local_legacy_cutover (
    slot INTEGER PRIMARY KEY CHECK (slot = 1) REFERENCES local_authority(slot),
    baseline BLOB NOT NULL,
    delta BLOB NOT NULL,
    projection BLOB NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS local_legacy_receipts (
    run INTEGER PRIMARY KEY,
    slot INTEGER NOT NULL DEFAULT 1 REFERENCES local_legacy_cutover(slot),
    start BLOB NOT NULL,
    result BLOB NOT NULL
) STRICT;
