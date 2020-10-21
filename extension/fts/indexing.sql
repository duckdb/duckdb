-- create schema for the index to live in
CREATE SCHEMA %fts_schema%;

-- create docs table with docid and name
CREATE TABLE %fts_schema%.docs AS (
    SELECT
        row_number() OVER (PARTITION BY(SELECT NULL)) AS docid,
        %input_id% AS name
    FROM
        %input_schema%.%input_table%
);

-- create terms table with term, docid, pos (term will be replace with termid later)
CREATE TABLE %fts_schema%.terms AS (
    SELECT
        term,
        docid,
        row_number() OVER (PARTITION BY docid) AS pos
    FROM (
        SELECT
            stem(unnest(string_split_regex(regexp_replace(lower(strip_accents(%input_val%)), '[^a-z]', ' ', 'g'), '\s+')), 'porter') AS term,
            row_number() OVER (PARTITION BY (SELECT NULL)) AS docid
        FROM %input_schema%.documents
    ) AS sq
    WHERE
        term != ''
);

-- add len column to docs table
ALTER TABLE %fts_schema%.docs ADD len INT;
UPDATE %fts_schema%.docs d
SET len = (
    SELECT count(term)
    FROM %fts_schema%.terms t
    WHERE t.docid = d.docid
);

-- create dict table with termid, term
CREATE TABLE %fts_schema%.dict AS
WITH distinct_terms AS (
    SELECT DISTINCT term, docid
    FROM %fts_schema%.terms
    ORDER BY docid
)
SELECT
    row_number() OVER (PARTITION BY (SELECT NULL)) AS termid,
    term
FROM
    distinct_terms;

-- add termid column to terms table and remove term column
ALTER TABLE %fts_schema%.terms ADD termid INT;
statement ok
UPDATE %fts_schema%.terms t
SET termid = (
    SELECT termid
    FROM %fts_schema%.dict d
    WHERE t.term = d.term
);
ALTER TABLE %fts_schema%.terms DROP term;

-- add df column to dict
ALTER TABLE %fts_schema%.dict ADD df INT;
UPDATE %fts_schema%.dict d
SET df = (
    SELECT count(distinct docid)
    FROM %fts_schema%.terms t
    WHERE d.termid = t.termid
    GROUP BY termid
);
