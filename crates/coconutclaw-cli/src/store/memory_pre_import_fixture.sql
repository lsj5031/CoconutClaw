-- Original bounded-memory table layout, before explicit import origins.
CREATE TABLE memory_records (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 scope TEXT NOT NULL,
 source_turn INTEGER NOT NULL REFERENCES turns(id),
 proposal_index INTEGER,
 curated INTEGER NOT NULL DEFAULT 0 CHECK(curated IN (0,1)),
 content TEXT NOT NULL CHECK(length(CAST(content AS BLOB)) BETWEEN 1 AND 2048),
 UNIQUE(source_turn, proposal_index)
);
CREATE TABLE memory_active(record_id INTEGER PRIMARY KEY REFERENCES memory_records(id));
CREATE VIRTUAL TABLE memory_search USING fts5(content);
CREATE TABLE memory_history (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 scope TEXT NOT NULL,
 source_turn INTEGER NOT NULL REFERENCES turns(id),
 kind TEXT NOT NULL CHECK(kind IN ('remember','promote','correct','undo')),
 old_record INTEGER REFERENCES memory_records(id),
 new_record INTEGER REFERENCES memory_records(id),
 undo_of INTEGER UNIQUE REFERENCES memory_history(id)
);
CREATE TABLE memory_command_receipts (
 scope TEXT NOT NULL, request_id TEXT NOT NULL, reply TEXT NOT NULL,
 PRIMARY KEY(scope,request_id)
);
INSERT INTO turns(id,ts,chat_id,input_type,user_text,provider_raw,status)
 VALUES(42,'fixture','telegram:321','system','synthetic remember','','memory_command');
INSERT INTO memory_records(id,scope,source_turn,curated,content)
 VALUES(101,'telegram:321',42,1,'retained synthetic orchard');
INSERT INTO memory_active VALUES(101);
INSERT INTO memory_search(rowid,content) VALUES(101,'retained synthetic orchard');
INSERT INTO memory_history(id,scope,source_turn,kind,new_record)
 VALUES(201,'telegram:321',42,'remember',101);
INSERT INTO memory_command_receipts VALUES('telegram:321','update:123','saved receipt');
