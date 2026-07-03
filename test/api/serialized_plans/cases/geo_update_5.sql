CREATE TABLE src (old_id VARCHAR, new_id VARCHAR, g GEOMETRY);
INSERT INTO src VALUES ('A', 'B', 'POINT(1 2)');
CREATE TABLE tgt (id INT, val VARCHAR, g GEOMETRY);
INSERT INTO tgt VALUES (1, 'A', 'POINT(3 4)');
UPDATE tgt SET val = src.new_id FROM src WHERE src.old_id = tgt.val;
