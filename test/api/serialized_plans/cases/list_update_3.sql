-- simple update, varchar column
CREATE TABLE t1 (id INT, val VARCHAR, tags VARCHAR[]);
INSERT INTO t1 VALUES (1, 'A', ARRAY['tag1', 'tag2']);
UPDATE t1 SET val = 'B' WHERE id = 1;
