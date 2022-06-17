BEGIN TRANSACTION;
-- test various types
CREATE TABLE integral_values (
    i TINYINT,
    j smallint,
    k integer,
    l bigint,
    m HUGEINT
);
INSERT INTO integral_values
    VALUES (1, 2, 3, 4, 5), (NULL, NULL, NULL, NULL, NULL);
CREATE TABLE numeric_values (
    i float,
    j double
);
INSERT INTO numeric_values
    VALUES (1.0, 3.0), (NULL, NULL);
CREATE TABLE decimal_values (
    i DECIMAL(4, 1),
    j DECIMAL(9, 2),
    k DECIMAL(18, 4),
    l DECIMAL(37, 2)
);
INSERT INTO decimal_values
    VALUES (1.0, 3.22, 10420942.4567, 12908124908190481290481.11), (NULL, NULL, NULL, NULL);
CREATE TABLE string_values (
    i varchar,
    j BLOB
);
INSERT INTO string_values
    VALUES ('hello world', '\xAA\xFF\xAA'), (NULL, NULL);
CREATE TABLE date_values (
    i date,
    j time,
    k timestamp
);
INSERT INTO date_values
    VALUES (date '1992-01-01', time '12:00:03', timestamp '1992-09-20 10:00:03'), (NULL, NULL, NULL);
CREATE TABLE uuid_values (
    u uuid
);
INSERT INTO uuid_values
    VALUES ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'), (NULL), ('47183823-2574-4bfd-b411-99ed177d3e43'), ('{10203040506070800102030405060708}');

CREATE TYPE int_alias AS INTEGER;
CREATE TYPE char_alias AS VARCHAR;
CREATE TABLE alias_values (
    i int_alias,
    j varchar,
    k char_alias
);
INSERT INTO alias_values
    VALUES (2, 'hello world', 'alias'), (NULL, NULL, NULL);
-- all types
CREATE TABLE all_types AS SELECT * FROM test_all_types();
-- test constraints
CREATE TABLE check_constraint (
    i integer,
    j integer,
    CHECK (i + j < 10)
);
CREATE TABLE not_null_constraint (
    i integer NOT NULL
);
CREATE TABLE pk_constraint (
    i integer,
    j integer,
    PRIMARY KEY (i, j)
);
CREATE TABLE unique_constraint (
    i integer UNIQUE
);
-- test schemas
CREATE SCHEMA test3;
-- test sequences
CREATE SEQUENCE test3.bla;
-- test various views
CREATE VIEW v1 AS
SELECT
    *
FROM
    integral_values;
CREATE VIEW test3.v2 AS
SELECT
    (i + 2) * 3
FROM
    integral_values;
-- bigger tables
CREATE TABLE big_integers AS
SELECT
    i
FROM
    RANGE (0,
        100000) t1 (i);
CREATE TABLE big_string AS
SELECT
    repeat('a', 100000) a;
COMMIT;

-- views
CREATE TABLE base_table AS SELECT * FROM range(4) tbl(i);

-- many expression types
CREATE VIEW cv1 AS
SELECT
       bt.k,
       (k*2)+1,
       CASE WHEN k=1 THEN 2 WHEN k=2 THEN 3 WHEN k=3 THEN 4 ELSE NULL END,
       k BETWEEN 1 AND 2,
       k::VARCHAR COLLATE NOCASE,
       (k=3 AND k+1=4) OR (k=2 AND k+1=3),
       NOT(k=3),
       #1,
       (SELECT 42),
       *,
       bt.*,
       k IN (1, 2, NULL),
       k IN (SELECT 1),
       'hello world',
       [1, 2, 3],
        {'x': [42, 88]},
       sum(k) over (partition by k order by k)
FROM base_table bt(k);

-- aggregates
CREATE VIEW cv2 AS
SELECT
    i%2 AS k,
    COUNT(*),
    COUNT(DISTINCT i%2),
    SUM(i),
    STRING_AGG(i ORDER BY i DESC NULLS FIRST),
    COUNT(i) FILTER (WHERE i=0)
FROM base_table
WHERE i<>1
GROUP BY 1
HAVING k<>1
ORDER BY 1;

-- window functions
CREATE VIEW cv3 AS
SELECT
    i,
    row_number() over () AS rownum,
    sum(i) over(partition by i%2 order by i asc),
    sum(i) over(partition by i%2 order by i desc nulls first),
    sum(i) over(order by rowid rows between 1 preceding and current row)
FROM base_table
QUALIFY row_number() over () != 2
ORDER BY 1;

-- recursive CTE
CREATE VIEW cv4 AS
WITH RECURSIVE cte AS (
    SELECT 1 i
    UNION ALL
    SELECT i+1
    FROM cte
    WHERE i < 3
)
SELECT * FROM cte;

-- multiple regular CTEs with setops
CREATE VIEW cv5 AS
WITH cte AS (
    SELECT 1 AS i
), cte2 AS (
    SELECT i + 1 AS i FROM cte
), cte3 AS (
    SELECT i + 1 AS i FROM cte2
)
SELECT * FROM cte
UNION ALL
SELECT * FROM cte2
UNION ALL
SELECT * FROM cte3
ORDER BY i;

-- various set ops
CREATE VIEW cv6 AS
SELECT * FROM generate_series(0, 6, 1) tbl(i)
INTERSECT
SELECT * FROM generate_series(0, 4, 1) tbl(i)
EXCEPT
(SELECT 1 UNION ALL SELECT 3)
ORDER BY 1;

-- group by all
CREATE VIEW cv7 AS
SELECT i % 2, SUM(i)
FROM base_table
GROUP BY ALL
ORDER BY ALL;

-- values
CREATE VIEW cv8 AS
VALUES (1), (2), (3), (NULL);

-- subqueries
CREATE VIEW cv9 AS
SELECT DISTINCT (SELECT tbl.i+1)
FROM (
    SELECT * FROM base_table WHERE i>1
) tbl(i)
ORDER BY (SELECT tbl.i);

-- samples
CREATE VIEW cv10 AS
SELECT *
FROM base_table TABLESAMPLE 10 ROWS
USING SAMPLE 100%
ORDER BY ALL;

-- grouping sets
CREATE VIEW cv11 AS
SELECT GROUPING_ID(k, i), i%2 AS k, i, SUM(i)
FROM base_table
GROUP BY GROUPING SETS ((), (k), (k, i))
ORDER BY ALL;

-- window clause
CREATE VIEW cv12 AS
SELECT
    i,
    sum(i) over(w)
FROM base_table
WINDOW w AS (partition by i%2 order by i asc)
ORDER BY ALL;

-- limit order by
CREATE VIEW cv13 AS
SELECT *
FROM base_table
ORDER BY i DESC
LIMIT 2
OFFSET 1;


-- v29: IGNORE NULLS
CREATE FUNCTION V29(x) AS LAST_VALUE(x IGNORE NULLS) OVER(ORDER BY x NULLS LAST);

FORCE CHECKPOINT;
