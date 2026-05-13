CREATE TABLE t1 (id INT, val VARCHAR, meta STRUCT(name VARCHAR, tags VARCHAR[]));
INSERT INTO t1 VALUES (1, 'A', {'name': 'foo', 'tags': ['t1', 't2']});
UPDATE t1 SET val = 'B' WHERE id = 1;
