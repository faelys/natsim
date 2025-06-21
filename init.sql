CREATE TABLE subjects
   (id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL);
CREATE UNIQUE INDEX i_subjects ON subjects(name);

CREATE TABLE received
   (timestamp REAL NOT NULL,
    subject_id INTEGER NOT NULL,
    data TEXT NOT NULL,
    FOREIGN KEY (subject_id) REFERENCES subjects (id));
CREATE INDEX i_rtime ON received(timestamp);
CREATE INDEX i_rsubjectid ON received(subject_id);

CREATE TABLE sent
   (timestamp REAL NOT NULL,
    subject_id INTEGER NOT NULL,
    data TEXT NOT NULL,
    FOREIGN KEY (subject_id) REFERENCES subjects (id));
CREATE INDEX i_stime ON received(timestamp);
CREATE INDEX i_ssubjectid ON received(subject_id);

CREATE VIEW received_view (timestamp,subject,data)
 AS SELECT datetime(timestamp),subjects.name,data
    FROM received LEFT OUTER JOIN subjects
    ON subjects.id = subject_id;

CREATE VIEW sent_view (timestamp,subject,data)
 AS SELECT datetime(timestamp),subjects.name,data
    FROM sent LEFT OUTER JOIN subjects ON subjects.id = subject_id;

CREATE TRIGGER insert_received INSTEAD OF INSERT ON received_view
BEGIN
    INSERT INTO subjects(name)
      SELECT NEW.subject WHERE NOT EXISTS
      (SELECT 1 FROM subjects WHERE name = NEW.subject);
    INSERT INTO received(timestamp,subject_id,data)
      VALUES (NEW.timestamp,
              (SELECT id FROM subjects WHERE name = NEW.subject),
              NEW.data);
END;

CREATE TRIGGER insert_sent INSTEAD OF INSERT ON sent_view
BEGIN
    INSERT INTO subjects(name)
      SELECT NEW.subject WHERE NOT EXISTS
      (SELECT 1 FROM subjects WHERE name = NEW.subject);
    INSERT INTO sent(timestamp,subject_id,data)
      VALUES (NEW.timestamp,
              (SELECT id FROM subjects WHERE name = NEW.subject),
              NEW.data);
END;

PRAGMA user_version = 1;
