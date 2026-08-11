CREATE TABLE acceptance_consumer_heartbeat (singleton INTEGER PRIMARY KEY CHECK(singleton=1), available INTEGER NOT NULL CHECK(available IN (0,1)), observed_at TEXT NOT NULL);
