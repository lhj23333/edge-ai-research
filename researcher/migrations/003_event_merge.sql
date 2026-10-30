ALTER TABLE events ADD COLUMN canonical_event_id INTEGER REFERENCES events(id);
ALTER TABLE events ADD COLUMN product_version TEXT NOT NULL DEFAULT '';
CREATE TABLE event_aliases (
 identity_key TEXT PRIMARY KEY, event_id INTEGER NOT NULL REFERENCES events(id)
);
INSERT INTO event_aliases(identity_key,event_id) SELECT identity_key,id FROM events;
