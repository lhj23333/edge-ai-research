CREATE INDEX feed_observations_item ON feed_observations(item_id,source_id);

ALTER TABLE feed_items ADD COLUMN event_visible_at TEXT;
UPDATE feed_items SET event_visible_at=substr(coalesce((
 SELECT r.created_at FROM event_cluster_decisions r
 WHERE r.item_id=feed_items.id AND r.final_event_id=feed_items.event_id ORDER BY r.id DESC LIMIT 1
),updated_at),1,19)||'.000000+00:00' WHERE state='clustered';
CREATE INDEX feed_items_visible ON feed_items(event_visible_at,event_id) WHERE state='clustered';
ALTER TABLE station_settings ADD COLUMN last_feed_visible_at TEXT;
UPDATE station_settings SET last_feed_visible_at=substr(last_visit_at,1,19)||'.000000+00:00' WHERE last_visit_at IS NOT NULL;

ALTER TABLE event_entities ADD COLUMN feed_only INTEGER NOT NULL DEFAULT 0;
ALTER TABLE event_topics ADD COLUMN feed_only INTEGER NOT NULL DEFAULT 0;

-- 旧记录只有在事件完全来自采集时才能确定关联依据。含独立证据的历史关联继续保留。
UPDATE event_entities SET feed_only=1 WHERE event_id IN (
 SELECT e.id FROM events e WHERE e.identity_key LIKE 'feed:%' AND NOT EXISTS (SELECT 1 FROM event_claims c WHERE c.event_id=e.id)
);
UPDATE event_topics SET feed_only=1 WHERE event_id IN (
 SELECT e.id FROM events e WHERE e.identity_key LIKE 'feed:%' AND NOT EXISTS (SELECT 1 FROM event_claims c WHERE c.event_id=e.id)
);

-- 已有关联缺少版本时，只回填能够从采集评估确认的唯一版本。
UPDATE document_task_links AS l SET recorded_version_id=(
 SELECT min(i.version_id) FROM feed_items i JOIN item_evaluations v ON v.item_id=i.id
 WHERE i.document_id=l.document_id AND v.task_id=l.task_id AND i.version_id IS NOT NULL
 GROUP BY i.document_id HAVING min(i.version_id)=max(i.version_id)
) WHERE l.recorded_version_id IS NULL AND l.task_id IN (SELECT id FROM tasks WHERE kind='feed');

UPDATE tasks SET status='failed',progress='评测未完成',error='部分样本未执行，评测未完成'
WHERE kind='selectbench' AND status='success'
  AND json_extract(result_json,'$.metrics.tested')+json_array_length(result_json,'$.errors')
      <json_extract(result_json,'$.metrics.total');
