CREATE TABLE feed_sources (
 id INTEGER PRIMARY KEY, source_key TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('rss','web_list','json_list','external')),
 tier TEXT NOT NULL CHECK(tier IN ('T1','T1.5','T2')), category TEXT NOT NULL DEFAULT '',
 url TEXT NOT NULL DEFAULT '', publisher_key TEXT NOT NULL, owner_entity_id INTEGER REFERENCES entities(id),
 config_json TEXT NOT NULL DEFAULT '{}', published_after TEXT, cursor_json TEXT NOT NULL DEFAULT '{}',
 enabled INTEGER NOT NULL DEFAULT 0, poll_minutes INTEGER NOT NULL DEFAULT 60,
 next_poll_at TEXT, consecutive_failures INTEGER NOT NULL DEFAULT 0,
 last_success_at TEXT, cadence_days REAL, cadence_stale INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX feed_sources_schedule ON feed_sources(enabled,next_poll_at);

CREATE TABLE feed_runs (
 id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL REFERENCES feed_sources(id),
 task_id INTEGER REFERENCES tasks(id), status TEXT NOT NULL DEFAULT 'running',
 fetched_count INTEGER NOT NULL DEFAULT 0, new_count INTEGER NOT NULL DEFAULT 0,
 gated_count INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '',
 started_at TEXT NOT NULL, finished_at TEXT, elapsed_ms INTEGER,
 UNIQUE(task_id,source_id)
);
CREATE INDEX feed_runs_source ON feed_runs(source_id,id DESC);

CREATE TABLE feed_items (
 id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL REFERENCES feed_sources(id),
 canonical_url TEXT NOT NULL UNIQUE, original_url TEXT NOT NULL, title TEXT NOT NULL,
 summary TEXT NOT NULL DEFAULT '', published_at TEXT, discovered_at TEXT NOT NULL,
 is_backfill INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'discovered'
 CHECK(state IN ('discovered','pending_date','gated_archive','prefiltered_out','scored_out','selected','clustered','duplicate')),
 gate_reason TEXT NOT NULL DEFAULT '', next_attempt_at TEXT, error TEXT NOT NULL DEFAULT '',
 tier TEXT NOT NULL CHECK(tier IN ('T1','T1.5','T2')), category TEXT NOT NULL DEFAULT '',
 publisher_key TEXT NOT NULL, owner_entity_id INTEGER REFERENCES entities(id),
 document_id INTEGER REFERENCES documents(id), version_id INTEGER REFERENCES document_versions(id),
 event_id INTEGER REFERENCES events(id), score REAL CHECK(score BETWEEN 0 AND 100),
 manual_score REAL CHECK(manual_score BETWEEN 0 AND 100), manual_reason TEXT NOT NULL DEFAULT '',
 output_json TEXT NOT NULL DEFAULT '{}', cluster_candidates_json TEXT, updated_at TEXT NOT NULL
);
CREATE INDEX feed_items_queue ON feed_items(state,next_attempt_at,id);
CREATE INDEX feed_items_event ON feed_items(event_id,published_at);
CREATE INDEX feed_items_document ON feed_items(document_id);
CREATE TABLE feed_observations (
 source_id INTEGER NOT NULL REFERENCES feed_sources(id), item_id INTEGER NOT NULL REFERENCES feed_items(id),
 seen_at TEXT NOT NULL, PRIMARY KEY(source_id,item_id)
);

CREATE TABLE item_evaluations (
 id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL REFERENCES feed_items(id),
 stage TEXT NOT NULL, prompt_version TEXT NOT NULL, route TEXT NOT NULL,
 task_id INTEGER NOT NULL REFERENCES tasks(id), status TEXT NOT NULL DEFAULT 'pending',
 score REAL, reason TEXT NOT NULL DEFAULT '', request_json TEXT NOT NULL, result_json TEXT NOT NULL DEFAULT '{}',
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(item_id,stage,prompt_version)
);
CREATE TABLE event_cluster_decisions (
 id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL REFERENCES feed_items(id),
 candidates_json TEXT NOT NULL DEFAULT '[]', similarity REAL, relation TEXT NOT NULL
 CHECK(relation IN ('same_event','follow_up','new_event')),
 reviewed INTEGER NOT NULL DEFAULT 0, manual INTEGER NOT NULL DEFAULT 0, reason TEXT NOT NULL DEFAULT '',
 final_event_id INTEGER NOT NULL REFERENCES events(id), created_at TEXT NOT NULL
);
CREATE INDEX event_cluster_item ON event_cluster_decisions(item_id,id);

ALTER TABLE events ADD COLUMN representative_document_id INTEGER REFERENCES documents(id);
ALTER TABLE events ADD COLUMN independent_source_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE events ADD COLUMN heat_score REAL NOT NULL DEFAULT 0;
ALTER TABLE events ADD COLUMN first_party_confirmed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE events ADD COLUMN cluster_locked INTEGER NOT NULL DEFAULT 0;
CREATE INDEX events_activity ON events(updated_at) WHERE state='active' AND canonical_event_id IS NULL;

CREATE TABLE daily_briefings (
 day TEXT PRIMARY KEY, created_at TEXT NOT NULL, event_ids_json TEXT NOT NULL,
 background_ids_json TEXT NOT NULL, revision_ids_json TEXT NOT NULL
);

ALTER TABLE usage ADD COLUMN route TEXT NOT NULL DEFAULT '';
ALTER TABLE usage ADD COLUMN evaluation_id INTEGER REFERENCES item_evaluations(id);
ALTER TABLE usage ADD COLUMN reserved_estimate REAL;
CREATE INDEX usage_route_day ON usage(day,route);
CREATE INDEX usage_created ON usage(created_at);
CREATE INDEX usage_evaluation ON usage(evaluation_id);

ALTER TABLE source_checks RENAME TO source_checks_original;
CREATE TABLE source_checks (
 id INTEGER PRIMARY KEY, document_id INTEGER UNIQUE REFERENCES documents(id),
 version_id INTEGER REFERENCES document_versions(id), feed_source_id INTEGER UNIQUE REFERENCES feed_sources(id),
 checked_at TEXT NOT NULL, final_url TEXT NOT NULL,
 etag TEXT NOT NULL DEFAULT '', last_modified TEXT NOT NULL DEFAULT '',
 CHECK((document_id IS NOT NULL AND version_id IS NOT NULL AND feed_source_id IS NULL)
    OR (document_id IS NULL AND version_id IS NULL AND feed_source_id IS NOT NULL))
);
INSERT INTO source_checks(document_id,version_id,checked_at,final_url,etag,last_modified)
 SELECT document_id,version_id,checked_at,final_url,etag,last_modified FROM source_checks_original;
DROP TABLE source_checks_original;

-- 旧简报的来源在升级时导入历史文档。已有文档保持当前版本与核验状态。
ALTER TABLE document_task_links ADD COLUMN recorded_version_id INTEGER REFERENCES document_versions(id);
INSERT OR IGNORE INTO documents(url,title,published_at,first_seen_at,last_fetched_at,extent,content,content_hash)
 SELECT s.url,s.title,s.published_at,s.fetched_at,s.fetched_at,s.extent,s.content,sha256(s.content)
 FROM sources s WHERE (lower(s.url) LIKE 'https://%' OR lower(s.url) LIKE 'http://%')
 AND NOT EXISTS (SELECT 1 FROM source_aliases a WHERE a.url=s.url)
 ORDER BY length(s.content)>0 DESC,s.id DESC;
INSERT OR IGNORE INTO document_versions(document_id,content_hash,content,extent,fetched_at,verification)
 SELECT coalesce(a.document_id,d.id),sha256(s.content),s.content,s.extent,s.fetched_at,'历史资料，未核验'
 FROM sources s LEFT JOIN documents d ON d.url=s.url LEFT JOIN source_aliases a ON a.url=s.url
 WHERE coalesce(a.document_id,d.id) IS NOT NULL;
INSERT INTO document_task_links(document_id,task_id,recorded_version_id)
 SELECT v.document_id,s.task_id,v.id FROM sources s LEFT JOIN documents d ON d.url=s.url
 LEFT JOIN source_aliases a ON a.url=s.url
 JOIN document_versions v ON v.document_id=coalesce(a.document_id,d.id) AND v.content_hash=sha256(s.content)
 WHERE v.id IS NOT NULL
 ON CONFLICT(document_id,task_id) DO UPDATE SET recorded_version_id=excluded.recorded_version_id;
INSERT OR IGNORE INTO source_aliases(url,document_id) SELECT url,id FROM documents;

UPDATE tasks SET status='interrupted',error='已切换为受控信源或会话式研究',
 finished_at=strftime('%Y-%m-%dT%H:%M:%S+00:00','now')
 WHERE status IN ('queued','running') AND (kind='scan' OR
 (session_id IS NULL AND kind IN ('research','chat','summary','onboard','question','revise')));
UPDATE research_sessions SET status='interrupted' WHERE status='running'
 AND id IN (SELECT session_id FROM tasks WHERE kind='scan' AND status='interrupted')
 AND id NOT IN (SELECT session_id FROM tasks WHERE session_id IS NOT NULL AND status IN ('queued','running'));
UPDATE collection_runs SET status='failed',details='已切换为受控信源',
 finished_at=strftime('%Y-%m-%dT%H:%M:%S+00:00','now') WHERE status='running';
UPDATE station_settings SET auto_enabled=0,next_run=NULL,last_attempt_at=NULL,last_success_at=NULL,
 last_status='never',last_error='' WHERE id=1;
