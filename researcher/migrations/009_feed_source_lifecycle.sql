ALTER TABLE feed_sources ADD COLUMN archived_at TEXT CHECK(archived_at IS NULL OR enabled=0);
CREATE INDEX feed_items_source ON feed_items(source_id);

-- 物理删除后仍保留操作记录，并防止自动初始化重新添加已删除的内置信源。
CREATE TABLE feed_source_lifecycle (
 id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, source_key TEXT NOT NULL,
 action TEXT NOT NULL CHECK(action IN ('archive','restore','delete')), changed_at TEXT NOT NULL
);
CREATE INDEX feed_source_lifecycle_source ON feed_source_lifecycle(source_id);
CREATE INDEX feed_source_lifecycle_deleted_key ON feed_source_lifecycle(source_key) WHERE action='delete';
