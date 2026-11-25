ALTER TABLE usage ADD COLUMN request_key TEXT;
ALTER TABLE usage ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 1;
ALTER TABLE usage ADD COLUMN estimated_input_tokens INTEGER;
ALTER TABLE usage ADD COLUMN estimated_output_tokens INTEGER;
ALTER TABLE usage ADD COLUMN auto_released_at TEXT;
CREATE INDEX usage_task ON usage(task_id,id);
WITH attempts AS (SELECT id,row_number() OVER (PARTITION BY task_id ORDER BY id) AS attempt FROM usage)
UPDATE usage SET attempt_count=attempts.attempt FROM attempts WHERE attempts.id=usage.id;
UPDATE usage SET auto_released_at=strftime('%Y-%m-%dT%H:%M:%S+00:00',created_at,'+12 minutes') WHERE status='reserved';
CREATE INDEX usage_active_reservations ON usage(auto_released_at) WHERE status='reserved';
CREATE VIEW model_call_ledger AS
SELECT id,task_id,request_key,attempt_count,provider,model,operation,route,created_at,day,status,
       coalesce(estimate,reserved_estimate) AS estimated_cost,
       coalesce(input_tokens,estimated_input_tokens)+coalesce(output_tokens,estimated_output_tokens) AS tokens,
       input_tokens,output_tokens,estimated_input_tokens,estimated_output_tokens,auto_released_at
FROM usage;

CREATE TABLE topic_retry_quotas (
 task_id INTEGER PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
 topic_id INTEGER REFERENCES topics(id) ON DELETE SET NULL,
 attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
 status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','finished','expired')),
 auto_released_at TEXT NOT NULL
);
CREATE INDEX topic_retry_expiry ON topic_retry_quotas(auto_released_at) WHERE status='active';
INSERT INTO topic_retry_quotas(task_id,topic_id,attempt_count,status,auto_released_at)
SELECT t.id,t.topic_id,(SELECT count(*) FROM usage u WHERE u.task_id=t.id),
       CASE WHEN t.status IN ('queued','running') THEN 'active' ELSE 'finished' END,
       strftime('%Y-%m-%dT%H:%M:%S+00:00','now','+30 minutes')
FROM tasks t WHERE t.automatic=1 AND t.schedule_key LIKE 'topic-revision:%';

CREATE TABLE content_cache (
 content_hash TEXT NOT NULL,
 namespace TEXT NOT NULL,
 value BLOB NOT NULL,
 PRIMARY KEY(content_hash,namespace)
) WITHOUT ROWID;

CREATE TABLE research_evidence_versions (
 target_kind TEXT NOT NULL CHECK(target_kind IN ('topic','event')),
 target_id INTEGER NOT NULL,
 request_hash TEXT NOT NULL,
 evidence_hash TEXT NOT NULL,
 task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
 PRIMARY KEY(target_kind,target_id,request_hash)
) WITHOUT ROWID;
ALTER TABLE knowledge_items ADD COLUMN evidence_hash TEXT;
ALTER TABLE daily_briefings ADD COLUMN body TEXT NOT NULL DEFAULT '';
ALTER TABLE daily_briefings ADD COLUMN polished TEXT NOT NULL DEFAULT '';
ALTER TABLE daily_briefings ADD COLUMN evidence_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE daily_briefings ADD COLUMN polish_attempted TEXT;
