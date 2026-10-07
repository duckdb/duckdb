CREATE TABLE nodes(id INTEGER PRIMARY KEY);
CREATE TABLE edges(here INTEGER, there INTEGER, PRIMARY KEY (here, there));

INSERT INTO nodes
SELECT i::INTEGER
FROM range(100000) nodes(i);

INSERT INTO edges
SELECT (component + here_level * 1000)::INTEGER,
       (component + there_level * 1000)::INTEGER
FROM range(1000) components(component),
     range(100) here_levels(here_level),
     range(100) there_levels(there_level)
WHERE here_level < there_level
  AND ((here_level * 17 + there_level * 31) % 7 = 0
       OR there_level = here_level + 1);

INSERT INTO edges
SELECT there, here
FROM edges;

INSERT INTO edges
SELECT id, id
FROM nodes;
