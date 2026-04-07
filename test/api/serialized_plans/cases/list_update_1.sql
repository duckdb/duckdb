-- updates varchar column
CREATE TABLE src (old_id VARCHAR, new_id VARCHAR);
INSERT INTO src VALUES ('A', 'B');
CREATE TABLE tgt (id INT, val VARCHAR, tags VARCHAR[]);
INSERT INTO tgt VALUES (1, 'A', ARRAY['tag1', 'tag2']);
UPDATE tgt SET val = src.new_id FROM src WHERE src.old_id = tgt.val;
