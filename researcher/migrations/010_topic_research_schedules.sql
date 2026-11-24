CREATE TABLE topic_research_schedules (
 topic_id INTEGER PRIMARY KEY REFERENCES topics(id) ON DELETE CASCADE,
 frequency TEXT NOT NULL CHECK (frequency IN ('daily','weekly')),
 schedule_time TEXT NOT NULL,
 weekday INTEGER NOT NULL CHECK (weekday BETWEEN 0 AND 6),
 next_run TEXT
);
