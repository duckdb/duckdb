CREATE TABLE li (a INT, b INT);
INSERT INTO li VALUES (1, 2), (3, 4), (5, NULL);
SELECT a, b, invoke(lambda x, y: x + y, a, b), invoke(lambda x: x * 2, a) FROM li ORDER BY a;
