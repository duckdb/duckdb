CREATE TABLE lf (i INT, l INT[]);
INSERT INTO lf VALUES (2, [1, 2, 3, 4]), (3, [4, 5, NULL, 6]), (4, NULL);
SELECT i, list_filter(l, lambda x: x > i), list_filter(l, lambda x, idx: idx % 2 = 0) FROM lf ORDER BY i;
