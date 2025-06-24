CREATE TABLE subjects
   (id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL);
CREATE UNIQUE INDEX i_subjects ON subjects(name);

CREATE TABLE received
   (id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    subject_id INTEGER NOT NULL,
    data TEXT NOT NULL,
    reply_subject_id INTEGER,
    FOREIGN KEY (subject_id) REFERENCES subjects (id),
    FOREIGN KEY (reply_subject_id) REFERENCES subjects (id));
CREATE INDEX i_rtime ON received(timestamp);
CREATE INDEX i_rsubjectid ON received(subject_id);

CREATE TABLE sent
   (timestamp REAL NOT NULL,
    subject_id INTEGER NOT NULL,
    data TEXT NOT NULL,
    reply_subject_id INTEGER,
    FOREIGN KEY (subject_id) REFERENCES subjects (id),
    FOREIGN KEY (reply_subject_id) REFERENCES subjects (id));
CREATE INDEX i_stime ON received(timestamp);
CREATE INDEX i_ssubjectid ON received(subject_id);

CREATE VIEW received_view (timestamp,subject,reply_subject,data)
 AS SELECT datetime(timestamp),subjects.name,rsub.name,data
    FROM received LEFT OUTER JOIN subjects ON subjects.id = subject_id
                  LEFT OUTER JOIN subjects rsub ON rsub.id = reply_subject_id;

CREATE VIEW sent_view (timestamp,subject,reply_subject,data)
 AS SELECT datetime(timestamp),subjects.name,rsub.name,data
    FROM sent LEFT OUTER JOIN subjects ON subjects.id = subject_id
              LEFT OUTER JOIN subjects rsub ON rsub.id = reply_subject_id;

CREATE TRIGGER insert_received INSTEAD OF INSERT ON received_view
BEGIN
    INSERT INTO subjects(name)
      SELECT NEW.subject WHERE NOT EXISTS
      (SELECT 1 FROM subjects WHERE name = NEW.subject);
    INSERT INTO subjects(name)
      SELECT NEW.reply_subject WHERE NOT EXISTS
      (SELECT 1 FROM subjects WHERE name = NEW.reply_subject)
      AND NEW.reply_subject <> '';
    INSERT INTO received(timestamp,subject_id,reply_subject_id,data)
      VALUES (NEW.timestamp,
              (SELECT id FROM subjects WHERE name = NEW.subject),
              (SELECT id FROM subjects WHERE name = NEW.reply_subject),
              NEW.data);
END;

CREATE TRIGGER insert_sent INSTEAD OF INSERT ON sent_view
BEGIN
    INSERT INTO subjects(name)
      SELECT NEW.subject WHERE NOT EXISTS
      (SELECT 1 FROM subjects WHERE name = NEW.subject);
    INSERT INTO subjects(name)
      SELECT NEW.reply_subject WHERE NOT EXISTS
      (SELECT 1 FROM subjects WHERE name = NEW.reply_subject)
      AND NEW.reply_subject <> '';
    INSERT INTO sent(timestamp,subject_id,reply_subject_id,data)
      VALUES (NEW.timestamp,
              (SELECT id FROM subjects WHERE name = NEW.subject),
              (SELECT id FROM subjects WHERE name = NEW.reply_subject),
              NEW.data);
END;

CREATE TABLE header_keys
   (id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL);
CREATE UNIQUE INDEX i_header_keys ON header_keys(name);

CREATE TABLE headers
   (id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_id INTEGER NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (key_id) REFERENCES header_keys (id));
CREATE UNIQUE INDEX i_headers ON headers(key_id, value);

CREATE VIEW headers_view (id,key,value)
 AS SELECT headers.id,header_keys.name,value
    FROM headers LEFT OUTER JOIN header_keys ON header_keys.id = key_id;

CREATE TRIGGER insert_header INSTEAD OF INSERT ON headers_view
BEGIN
    INSERT INTO header_keys(name)
      SELECT NEW.key WHERE NOT EXISTS
      (SELECT 1 FROM header_keys WHERE name = NEW.key);
    INSERT INTO headers(id,key_id,value)
      VALUES (NEW.id,
              (SELECT id FROM header_keys WHERE name = NEW.key),
              NEW.value);
END;

CREATE TABLE received_headers
   (msg_id INTEGER NOT NULL,
    header_id INTEGER NOT NULL,
    FOREIGN KEY (msg_id) REFERENCES received (id),
    FOREIGN KEY (header_id) REFERENCES headers (id));

CREATE VIEW received_headers_view (msg_id,key,value)
 AS SELECT msg_id,header_keys.name,headers.value
    FROM received_headers
         LEFT OUTER JOIN headers ON headers.id = header_id
         LEFT OUTER JOIN header_keys ON header_keys.id = headers.key_id;

CREATE TRIGGER insert_received_header INSTEAD OF INSERT ON received_headers_view
BEGIN
    INSERT INTO headers_view(key,value)
      SELECT NEW.key,NEW.value WHERE NOT EXISTS
      (SELECT 1 FROM headers_view WHERE key = NEW.key AND value = NEW.value);
    INSERT INTO received_headers(msg_id,header_id)
      VALUES (NEW.msg_id,
              (SELECT id FROM headers_view WHERE key = NEW.key AND value = NEW.value));
END;

PRAGMA user_version = 1;
