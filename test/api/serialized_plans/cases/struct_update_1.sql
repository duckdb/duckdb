CREATE TABLE src (old_id VARCHAR, new_id VARCHAR);
INSERT INTO src VALUES ('A', 'B');
CREATE TABLE tgt (id INT, val VARCHAR, meta STRUCT(name VARCHAR, tags VARCHAR[]));
INSERT INTO tgt VALUES (1, 'A', {'name': 'foo', 'tags': ['t1', 't2']});
UPDATE tgt SET val = src.new_id FROM src WHERE src.old_id = tgt.val;
