CREATE TABLE aoc2022_day07_tree(node INTEGER, parent INTEGER, is_directory BOOLEAN, size BIGINT);

INSERT INTO aoc2022_day07_tree
SELECT node::INTEGER,
       CASE WHEN node = 0 THEN NULL ELSE ((node - 1) // 4)::INTEGER END,
       node < 4096,
       CASE WHEN node < 4096 THEN NULL ELSE (node * 17 + 3)::BIGINT END
FROM range(16384) nodes(node);
