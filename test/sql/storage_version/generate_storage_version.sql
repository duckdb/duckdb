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

-- generate a bunch of stuff in the WAL just to force a checkpoint
-- FIXME: this really needs to be done better

UPDATE
    big_integers
SET
    i = i + 1;

UPDATE
    big_integers
SET
    i = i + 1;

UPDATE
    big_integers
SET
    i = i + 1;

UPDATE
    big_integers
SET
    i = i + 1;

UPDATE
    big_integers
SET
    i = i + 1;

UPDATE
    big_integers
SET
    i = i - 5;
