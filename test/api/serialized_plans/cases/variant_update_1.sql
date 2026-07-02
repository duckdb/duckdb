CREATE TABLE src (old_id VARCHAR, new_id VARCHAR);
INSERT INTO src VALUES ('A', 'B');
CREATE TABLE tgt (id INT, val VARCHAR, data VARIANT);
INSERT INTO tgt VALUES (1, 'A', 42::VARIANT);
UPDATE tgt SET val = src.new_id FROM src WHERE src.old_id = tgt.val;
