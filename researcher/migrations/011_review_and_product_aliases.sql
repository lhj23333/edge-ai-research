ALTER TABLE feed_items ADD COLUMN manual_release INTEGER NOT NULL DEFAULT 0 CHECK(manual_release IN (0,1));
ALTER TABLE feed_items ADD COLUMN manual_target_event_id INTEGER REFERENCES events(id);
CREATE TABLE feed_manual_reviews (
 id INTEGER PRIMARY KEY,
 item_id INTEGER NOT NULL REFERENCES feed_items(id),
 action TEXT NOT NULL CHECK(action IN ('release','score','assignment')),
 reason TEXT NOT NULL,
 score REAL,
 event_id INTEGER REFERENCES events(id),
 created_at TEXT NOT NULL
);
CREATE INDEX feed_manual_reviews_item ON feed_manual_reviews(item_id,id);

ALTER TABLE entities ADD COLUMN canonical_entity_id INTEGER REFERENCES entities(id);
CREATE INDEX entities_canonical ON entities(canonical_entity_id);
CREATE TABLE entity_aliases (
 normalized_name TEXT PRIMARY KEY,
 name TEXT NOT NULL,
 entity_id INTEGER NOT NULL REFERENCES entities(id)
);
CREATE TABLE entity_claims (
 entity_id INTEGER NOT NULL REFERENCES entities(id),
 claim_id INTEGER NOT NULL REFERENCES claims(id),
 PRIMARY KEY(entity_id,claim_id)
);

CREATE TABLE document_fetch_failures (
 document_id INTEGER PRIMARY KEY REFERENCES documents(id),
 checked_at TEXT NOT NULL,
 error TEXT NOT NULL
);
