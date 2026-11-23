CREATE TABLE source_aliases (
 url TEXT PRIMARY KEY, document_id INTEGER NOT NULL REFERENCES documents(id)
);
INSERT INTO source_aliases SELECT url,id FROM documents;
CREATE TABLE source_checks (
 document_id INTEGER PRIMARY KEY REFERENCES documents(id),
 version_id INTEGER NOT NULL REFERENCES document_versions(id),
 checked_at TEXT NOT NULL, final_url TEXT NOT NULL,
 etag TEXT NOT NULL DEFAULT '', last_modified TEXT NOT NULL DEFAULT ''
);
CREATE TABLE research_cache (
 cache_key TEXT PRIMARY KEY, kind TEXT NOT NULL, context_key TEXT NOT NULL,
 inputs_json TEXT NOT NULL, outputs_json TEXT NOT NULL DEFAULT '{}',
 task_id INTEGER NOT NULL REFERENCES tasks(id), status TEXT NOT NULL,
 checked_at TEXT, updated_at TEXT NOT NULL
);
CREATE INDEX research_cache_context ON research_cache(kind,context_key,status,checked_at);
ALTER TABLE knowledge_items ADD COLUMN canonical_item_id INTEGER REFERENCES knowledge_items(id);
CREATE TABLE knowledge_facets (
 item_id INTEGER NOT NULL REFERENCES knowledge_items(id), facet_key TEXT NOT NULL,
 kind TEXT NOT NULL, conditions_json TEXT NOT NULL, topic_id INTEGER REFERENCES topics(id),
 entity_id INTEGER REFERENCES entities(id), PRIMARY KEY(item_id,facet_key)
);
INSERT INTO knowledge_facets SELECT id,identity_key,kind,conditions_json,topic_id,entity_id FROM knowledge_items;
CREATE TABLE knowledge_changes (
 task_id INTEGER NOT NULL REFERENCES tasks(id), item_id INTEGER NOT NULL REFERENCES knowledge_items(id),
 change_type TEXT NOT NULL, related_id INTEGER REFERENCES knowledge_items(id), reason TEXT NOT NULL,
 PRIMARY KEY(task_id,item_id,change_type)
);
CREATE TABLE evidence_processing (
 chunk_id INTEGER NOT NULL REFERENCES document_chunks(id), context_key TEXT NOT NULL,
 task_id INTEGER NOT NULL REFERENCES tasks(id), status TEXT NOT NULL,
 PRIMARY KEY(chunk_id,context_key)
);
CREATE TABLE claims_new (
 id INTEGER PRIMARY KEY, document_id INTEGER REFERENCES documents(id),
 document_version_id INTEGER REFERENCES document_versions(id),
 statement TEXT NOT NULL, excerpt TEXT NOT NULL DEFAULT '',
 label TEXT NOT NULL CHECK(label IN ('公开披露','厂商宣称','分析判断','待核实')),
 verification TEXT NOT NULL DEFAULT '待核实', created_at TEXT NOT NULL, verified_at TEXT,
 UNIQUE(document_version_id,statement,label)
);
INSERT INTO claims_new SELECT * FROM claims;
DROP TABLE claims;
ALTER TABLE claims_new RENAME TO claims;
CREATE INDEX claims_document ON claims(document_id);
