CREATE TABLE nodes(node INTEGER PRIMARY KEY);
CREATE TABLE edges("from" INTEGER, "to" INTEGER, PRIMARY KEY ("from", "to"));

INSERT INTO nodes
SELECT (copy * 16 + node)::INTEGER
FROM range(20) copies(copy), range(1, 17) base_nodes(node);

INSERT INTO edges
SELECT (copy * 16 + base_edge."from")::INTEGER,
       (copy * 16 + base_edge."to")::INTEGER
FROM range(20) copies(copy),
     (VALUES
         (1, 2), (1, 3),
         (2, 3), (2, 9),
         (4, 5), (4, 6), (4, 10), (4, 13),
         (5, 6), (5, 7), (5, 10), (5, 14),
         (6, 7), (6, 8), (6, 9), (6, 11),
         (7, 8),
         (11, 12), (11, 15),
         (15, 16)
     ) AS base_edge("from", "to");

INSERT INTO edges
SELECT "to", "from"
FROM edges;
