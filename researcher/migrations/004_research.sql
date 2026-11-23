ALTER TABLE tasks ADD COLUMN session_id INTEGER REFERENCES research_sessions(id);
ALTER TABLE tasks ADD COLUMN automatic INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN request_limit INTEGER;

CREATE TABLE research_sessions (
 id INTEGER PRIMARY KEY, title TEXT NOT NULL, purpose TEXT NOT NULL,
 scope_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'ready',
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE research_messages (
 id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL REFERENCES research_sessions(id),
 task_id INTEGER REFERENCES tasks(id), role TEXT NOT NULL, body TEXT NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE research_operations (
 id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL REFERENCES tasks(id),
 operation_key TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL,
 inputs_json TEXT NOT NULL, outputs_json TEXT NOT NULL DEFAULT '{}',
 elapsed_ms INTEGER, error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(task_id,operation_key)
);
CREATE TABLE source_snapshots (
 id INTEGER PRIMARY KEY, version_id INTEGER NOT NULL REFERENCES document_versions(id),
 content_type TEXT NOT NULL, content_hash TEXT NOT NULL, body BLOB NOT NULL,
 headers_json TEXT NOT NULL DEFAULT '{}', method TEXT NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(version_id,content_hash)
);
CREATE TABLE document_chunks (
 id INTEGER PRIMARY KEY, version_id INTEGER NOT NULL REFERENCES document_versions(id),
 position INTEGER NOT NULL, start_offset INTEGER NOT NULL, end_offset INTEGER NOT NULL,
 page INTEGER, heading TEXT NOT NULL DEFAULT '', content TEXT NOT NULL,
 content_hash TEXT NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(version_id,position)
);
CREATE TABLE ingestion_queue (
 version_id INTEGER PRIMARY KEY REFERENCES document_versions(id), task_id INTEGER REFERENCES tasks(id),
 status TEXT NOT NULL DEFAULT 'pending', detail TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL
);
INSERT INTO ingestion_queue(version_id,updated_at) SELECT id,fetched_at FROM document_versions;
CREATE TABLE claim_evidence (
 claim_id INTEGER NOT NULL REFERENCES claims(id), chunk_id INTEGER NOT NULL REFERENCES document_chunks(id),
 task_id INTEGER REFERENCES tasks(id), verdict TEXT NOT NULL, reason TEXT NOT NULL,
 created_at TEXT NOT NULL, PRIMARY KEY(claim_id,chunk_id)
);
CREATE TABLE knowledge_items (
 id INTEGER PRIMARY KEY, identity_key TEXT NOT NULL UNIQUE, kind TEXT NOT NULL,
 body TEXT NOT NULL, conditions_json TEXT NOT NULL DEFAULT '{}',
 status TEXT NOT NULL, topic_id INTEGER REFERENCES topics(id), entity_id INTEGER REFERENCES entities(id),
 revision INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE knowledge_versions (
 id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL REFERENCES knowledge_items(id),
 version INTEGER NOT NULL, body TEXT NOT NULL, status TEXT NOT NULL,
 task_id INTEGER REFERENCES tasks(id), reason TEXT NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(item_id,version)
);
CREATE TABLE knowledge_evidence (
 item_id INTEGER NOT NULL REFERENCES knowledge_items(id), claim_id INTEGER NOT NULL REFERENCES claims(id),
 PRIMARY KEY(item_id,claim_id)
);
CREATE TABLE research_gaps (
 id INTEGER PRIMARY KEY, identity_key TEXT NOT NULL UNIQUE, question TEXT NOT NULL,
 reason TEXT NOT NULL, completion TEXT NOT NULL, priority INTEGER NOT NULL,
 target_kind TEXT NOT NULL DEFAULT 'domain', target_id INTEGER,
 origin_task_id INTEGER REFERENCES tasks(id), task_id INTEGER REFERENCES tasks(id),
 evidence_json TEXT NOT NULL DEFAULT '[]', status TEXT NOT NULL DEFAULT 'open',
 attempts INTEGER NOT NULL DEFAULT 0, next_check_at TEXT NOT NULL,
 automatic_allowed INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE research_feedback (
 id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL REFERENCES research_sessions(id),
 task_id INTEGER REFERENCES tasks(id), item_id INTEGER REFERENCES knowledge_items(id),
 kind TEXT NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE research_experiences (
 id INTEGER PRIMARY KEY, query_key TEXT NOT NULL UNIQUE, query TEXT NOT NULL,
 scope_json TEXT NOT NULL, document_ids_json TEXT NOT NULL DEFAULT '[]',
 useful_count INTEGER NOT NULL DEFAULT 0, failure_count INTEGER NOT NULL DEFAULT 0,
 last_result TEXT NOT NULL, last_used_at TEXT NOT NULL
);
CREATE INDEX chunks_version ON document_chunks(version_id);
CREATE INDEX evidence_chunk ON claim_evidence(chunk_id);
CREATE INDEX research_messages_session ON research_messages(session_id,id);
CREATE INDEX research_gaps_pending ON research_gaps(status,next_check_at,priority);
CREATE INDEX research_ops_task ON research_operations(task_id,status);
