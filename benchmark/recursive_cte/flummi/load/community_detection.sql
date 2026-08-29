CREATE TABLE nodes(id INTEGER PRIMARY KEY);
CREATE TABLE edges(here INTEGER, there INTEGER, PRIMARY KEY (here, there));

INSERT INTO nodes
SELECT i::INTEGER
FROM range(10000) nodes(i);

INSERT INTO edges
SELECT node.id,
       ((node.id // 50) * 50 + (node.id % 50 + edge_offset) % 50)::INTEGER
FROM nodes AS node, range(1, 3) offsets(edge_offset);
