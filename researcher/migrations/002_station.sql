ALTER TABLE topics ADD COLUMN research_state TEXT NOT NULL DEFAULT 'historical';
ALTER TABLE tasks ADD COLUMN job_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE tasks ADD COLUMN result_json TEXT NOT NULL DEFAULT '{}';

CREATE TABLE documents (
 id INTEGER PRIMARY KEY, url TEXT NOT NULL UNIQUE, title TEXT NOT NULL,
 publisher TEXT NOT NULL DEFAULT '', published_at TEXT, first_seen_at TEXT NOT NULL,
 last_fetched_at TEXT NOT NULL, extent TEXT NOT NULL, content TEXT NOT NULL DEFAULT '',
 content_hash TEXT NOT NULL, verified_at TEXT, verification TEXT NOT NULL DEFAULT '未核验'
);
CREATE TABLE document_task_links (
 document_id INTEGER NOT NULL REFERENCES documents(id), task_id INTEGER NOT NULL REFERENCES tasks(id),
 PRIMARY KEY(document_id,task_id)
);
CREATE TABLE document_versions (
 id INTEGER PRIMARY KEY, document_id INTEGER NOT NULL REFERENCES documents(id),
 content_hash TEXT NOT NULL, content TEXT NOT NULL, extent TEXT NOT NULL,
 fetched_at TEXT NOT NULL, verified_at TEXT, verification TEXT NOT NULL,
 UNIQUE(document_id,content_hash)
);
CREATE TABLE claims (
 id INTEGER PRIMARY KEY, document_id INTEGER REFERENCES documents(id),
 document_version_id INTEGER REFERENCES document_versions(id),
 statement TEXT NOT NULL, excerpt TEXT NOT NULL DEFAULT '',
 label TEXT NOT NULL CHECK(label IN ('公开披露','厂商宣称','分析判断','待核实')),
 verification TEXT NOT NULL DEFAULT '待核实', created_at TEXT NOT NULL,
 verified_at TEXT, UNIQUE(document_id,statement,label)
);
CREATE TABLE entities (
 id INTEGER PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('厂商','产品','应用场景','技术路线','软件生态')),
 name TEXT NOT NULL, normalized_name TEXT NOT NULL, summary TEXT NOT NULL DEFAULT '',
 parent_id INTEGER REFERENCES entities(id), state TEXT NOT NULL DEFAULT 'candidate',
 followed INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(kind,normalized_name)
);
CREATE TABLE entity_fields (
 id INTEGER PRIMARY KEY, entity_id INTEGER NOT NULL REFERENCES entities(id),
 field_name TEXT NOT NULL, value TEXT NOT NULL, claim_id INTEGER REFERENCES claims(id),
 updated_at TEXT NOT NULL, UNIQUE(entity_id,field_name,value)
);
CREATE TABLE relations (
 id INTEGER PRIMARY KEY, from_entity_id INTEGER NOT NULL REFERENCES entities(id),
 to_entity_id INTEGER NOT NULL REFERENCES entities(id), relation_type TEXT NOT NULL,
 explanation TEXT NOT NULL, claim_id INTEGER REFERENCES claims(id), created_at TEXT NOT NULL,
 UNIQUE(from_entity_id,to_entity_id,relation_type)
);
CREATE TABLE events (
 id INTEGER PRIMARY KEY, identity_key TEXT NOT NULL UNIQUE, event_type TEXT NOT NULL,
 title TEXT NOT NULL, what_happened TEXT NOT NULL, why_important TEXT NOT NULL,
 competitive_impact TEXT NOT NULL DEFAULT '', occurred_at TEXT, published_at TEXT,
 importance INTEGER NOT NULL DEFAULT 1, state TEXT NOT NULL DEFAULT 'active',
 background INTEGER NOT NULL DEFAULT 0, followup_event_id INTEGER REFERENCES events(id),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE event_claims (
 event_id INTEGER NOT NULL REFERENCES events(id), claim_id INTEGER NOT NULL REFERENCES claims(id),
 PRIMARY KEY(event_id,claim_id)
);
CREATE TABLE event_entities (
 event_id INTEGER NOT NULL REFERENCES events(id), entity_id INTEGER NOT NULL REFERENCES entities(id),
 PRIMARY KEY(event_id,entity_id)
);
CREATE TABLE event_topics (
 event_id INTEGER NOT NULL REFERENCES events(id), topic_id INTEGER NOT NULL REFERENCES topics(id),
 impact TEXT NOT NULL DEFAULT '', PRIMARY KEY(event_id,topic_id)
);
CREATE TABLE judgments (
 id INTEGER PRIMARY KEY, topic_id INTEGER NOT NULL REFERENCES topics(id),
 purpose TEXT NOT NULL CHECK(purpose IN ('行业竞争','部署决策')),
 status TEXT NOT NULL DEFAULT '待核实', current_version_id INTEGER,
 UNIQUE(topic_id,purpose)
);
CREATE TABLE judgment_versions (
 id INTEGER PRIMARY KEY, judgment_id INTEGER NOT NULL REFERENCES judgments(id),
 version INTEGER NOT NULL, conclusion TEXT NOT NULL, status TEXT NOT NULL,
 boundaries TEXT NOT NULL DEFAULT '', unresolved TEXT NOT NULL DEFAULT '',
 reason TEXT NOT NULL, event_id INTEGER REFERENCES events(id), created_at TEXT NOT NULL,
 UNIQUE(judgment_id,version)
);
CREATE TABLE judgment_evidence (
 version_id INTEGER NOT NULL REFERENCES judgment_versions(id), claim_id INTEGER NOT NULL REFERENCES claims(id),
 stance TEXT NOT NULL CHECK(stance IN ('支持','反对')),
 PRIMARY KEY(version_id,claim_id,stance)
);
CREATE TABLE research_questions (
 id INTEGER PRIMARY KEY, target_kind TEXT NOT NULL, target_id INTEGER,
 purpose TEXT NOT NULL CHECK(purpose IN ('行业竞争','部署决策')),
 main_question TEXT NOT NULL, subquestions_json TEXT NOT NULL DEFAULT '[]',
 conditions_json TEXT NOT NULL DEFAULT '{}', selected_note_ids_json TEXT NOT NULL DEFAULT '[]',
 completion TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'ready', task_id INTEGER REFERENCES tasks(id),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE task_steps (
 id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL REFERENCES tasks(id), stage TEXT NOT NULL,
 status TEXT NOT NULL, inputs_json TEXT NOT NULL DEFAULT '{}', outputs_json TEXT NOT NULL DEFAULT '{}',
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(task_id,stage)
);
CREATE TABLE collection_runs (
 id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL UNIQUE REFERENCES tasks(id),
 window_start TEXT, window_end TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'running',
 scope_json TEXT NOT NULL, details TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL,
 finished_at TEXT
);
CREATE TABLE coverage_checks (
 id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL REFERENCES collection_runs(id),
 target TEXT NOT NULL, query TEXT NOT NULL, status TEXT NOT NULL, result_count INTEGER NOT NULL DEFAULT 0,
 details TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL
);
CREATE TABLE daily_issues (
 id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL UNIQUE REFERENCES collection_runs(id),
 coverage_start TEXT, coverage_end TEXT NOT NULL, status TEXT NOT NULL,
 event_ids_json TEXT NOT NULL DEFAULT '[]', revision_ids_json TEXT NOT NULL DEFAULT '[]',
 next_questions_json TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL
);
CREATE TABLE annotations (
 id INTEGER PRIMARY KEY, target_kind TEXT NOT NULL, target_id INTEGER NOT NULL,
 body TEXT NOT NULL, origin_message_id INTEGER UNIQUE REFERENCES messages(id),
 archived INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE annotation_versions (
 id INTEGER PRIMARY KEY, annotation_id INTEGER NOT NULL REFERENCES annotations(id),
 version INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(annotation_id,version)
);
CREATE TABLE station_settings (
 id INTEGER PRIMARY KEY CHECK(id=1), watchlist_confirmed INTEGER NOT NULL DEFAULT 0,
 sources_confirmed INTEGER NOT NULL DEFAULT 0, auto_enabled INTEGER NOT NULL DEFAULT 0,
 schedule_time TEXT NOT NULL DEFAULT '09:00', next_run TEXT, last_attempt_at TEXT,
 last_success_at TEXT, last_status TEXT NOT NULL DEFAULT 'never', last_error TEXT NOT NULL DEFAULT '',
 last_visit_at TEXT, coverage_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE watchlist_proposals (
 id INTEGER PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL,
 reason TEXT NOT NULL, document_id INTEGER REFERENCES documents(id),
 status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL,
 UNIQUE(kind,name)
);
CREATE INDEX events_published ON events(published_at);
CREATE INDEX event_topics_topic ON event_topics(topic_id);
CREATE INDEX claims_document ON claims(document_id);
CREATE INDEX annotations_target ON annotations(target_kind,target_id);

INSERT INTO station_settings(id) VALUES(1);
INSERT INTO annotations(target_kind,target_id,body,origin_message_id,created_at,updated_at)
 SELECT 'topic',topic_id,body,id,created_at,created_at FROM messages WHERE role='observation';
INSERT INTO annotation_versions(annotation_id,version,body,created_at)
 SELECT a.id,1,a.body,a.created_at FROM annotations a WHERE a.origin_message_id IS NOT NULL;
UPDATE topics SET schedule='manual',next_run=NULL;
