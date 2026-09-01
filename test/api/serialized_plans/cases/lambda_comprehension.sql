CREATE TABLE lc (i INT, l INT[]);
INSERT INTO lc VALUES (1, [1, 2, 3, 4]), (2, [5, 6]), (3, [7]);
SELECT i, [x + i FOR x IN l IF x % 2 = 0] FROM lc ORDER BY i;
