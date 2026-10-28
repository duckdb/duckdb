CREATE TABLE lt (i INT, l INT[]);
INSERT INTO lt VALUES (10, [1, 2, 3]), (20, [4, 5]), (30, NULL);
SELECT i, list_transform(l, lambda x: x + i), list_transform(l, lambda x, idx: x * idx), list_transform(l, lambda e: list_transform([e, e + 1], lambda x: x + i)) FROM lt ORDER BY i;
