CREATE TABLE topics (
 id INTEGER PRIMARY KEY, title TEXT NOT NULL, question TEXT NOT NULL,
 kind TEXT NOT NULL DEFAULT '专题', created_at TEXT NOT NULL,
 schedule TEXT NOT NULL DEFAULT 'manual', schedule_time TEXT NOT NULL DEFAULT '09:00',
 weekday INTEGER NOT NULL DEFAULT 0, next_run TEXT, last_checked TEXT
);
CREATE TABLE tasks (
 id INTEGER PRIMARY KEY, topic_id INTEGER REFERENCES topics(id),
 kind TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued', prompt TEXT NOT NULL DEFAULT '',
 profile_id TEXT NOT NULL, profile_fingerprint TEXT NOT NULL, provider TEXT NOT NULL, model TEXT NOT NULL,
 created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT, heartbeat TEXT,
 progress TEXT NOT NULL DEFAULT '等待后台执行', error TEXT NOT NULL DEFAULT '',
 cancel_requested INTEGER NOT NULL DEFAULT 0, result_text TEXT NOT NULL DEFAULT '',
 context_brief INTEGER, schedule_key TEXT UNIQUE
);
CREATE UNIQUE INDEX one_active_task ON tasks(topic_id) WHERE status IN ('queued','running');
CREATE TABLE sources (
 id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL REFERENCES tasks(id),
 title TEXT NOT NULL, url TEXT NOT NULL, content TEXT NOT NULL DEFAULT '',
 extent TEXT NOT NULL, published_at TEXT, fetched_at TEXT NOT NULL,
 UNIQUE(task_id,url)
);
CREATE TABLE briefs (
 id INTEGER PRIMARY KEY, topic_id INTEGER NOT NULL REFERENCES topics(id),
 task_id INTEGER NOT NULL UNIQUE REFERENCES tasks(id), title TEXT NOT NULL,
 body TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE messages (
 id INTEGER PRIMARY KEY, topic_id INTEGER NOT NULL REFERENCES topics(id),
 task_id INTEGER REFERENCES tasks(id), brief_id INTEGER REFERENCES briefs(id),
 role TEXT NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE artifacts (
 id INTEGER PRIMARY KEY, topic_id INTEGER NOT NULL REFERENCES topics(id),
 title TEXT NOT NULL, body TEXT NOT NULL, version INTEGER NOT NULL,
 parent_id INTEGER REFERENCES artifacts(id), origin_task INTEGER REFERENCES tasks(id),
 created_at TEXT NOT NULL
);
CREATE TABLE usage (
 id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL REFERENCES tasks(id),
 profile_id TEXT NOT NULL, provider TEXT NOT NULL, model TEXT NOT NULL,
 operation TEXT NOT NULL, created_at TEXT NOT NULL, day TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'reserved', input_tokens INTEGER, output_tokens INTEGER,
 estimate REAL, price_note TEXT NOT NULL DEFAULT ''
);
CREATE INDEX usage_day ON usage(day);
CREATE TABLE runtime (name TEXT PRIMARY KEY, value TEXT NOT NULL);
