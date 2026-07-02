-- simple update, varchar column
CREATE TABLE t1 (id INT, val VARCHAR, data VARIANT);
INSERT INTO t1 VALUES (1, 'A', 42::VARIANT);
UPDATE t1 SET val = 'B' WHERE id = 1;
