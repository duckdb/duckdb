DROP SCHEMA IF EXISTS %fts_schema% CASCADE;
CREATE SCHEMA %fts_schema%;
CREATE MACRO %fts_schema%.tokenize(s) AS stem(unnest(string_split_regex(regexp_replace(lower(strip_accents(s)), '[^a-z]', ' ', 'g'), '\s+')), '%stemmer%');

CREATE TABLE %fts_schema%.docs AS (
    SELECT
        row_number() OVER (PARTITION BY(SELECT NULL)) AS docid,
        %input_id% AS name
    FROM
        %input_schema%.%input_table%
);

CREATE TABLE %fts_schema%.terms AS (
    SELECT
        term,
        docid,
        row_number() OVER (PARTITION BY docid) AS pos
    FROM (
        SELECT
            %fts_schema%.tokenize(%input_val%) AS term,
            row_number() OVER (PARTITION BY (SELECT NULL)) AS docid
        FROM %input_schema%.%input_table%
    ) AS sq
    WHERE
        term != ''
);

ALTER TABLE %fts_schema%.docs ADD len INT;
UPDATE %fts_schema%.docs d
SET len = (
    SELECT count(term)
    FROM %fts_schema%.terms t
    WHERE t.docid = d.docid
);

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

ALTER TABLE %fts_schema%.terms ADD termid INT;
UPDATE %fts_schema%.terms t
SET termid = (
    SELECT termid
    FROM %fts_schema%.dict d
    WHERE t.term = d.term
);
ALTER TABLE %fts_schema%.terms DROP term;

ALTER TABLE %fts_schema%.dict ADD df INT;
UPDATE %fts_schema%.dict d
SET df = (
    SELECT count(distinct docid)
    FROM %fts_schema%.terms t
    WHERE d.termid = t.termid
    GROUP BY termid
);

CREATE TABLE %fts_schema%.stats AS (
    SELECT COUNT(docs.docid) AS num_docs, SUM(docs.len) / COUNT(docs.len) AS avgdl
    FROM %fts_schema%.docs AS docs
);

CREATE MACRO %fts_schema%.match_bm25(docname, query_string, k=1.2, b=0.75, conjunctive=0) AS docname IN (
    WITH tokens AS
        (SELECT DISTINCT %fts_schema%.tokenize(query_string) AS t),
    qtermids AS
        (SELECT termid FROM %fts_schema%.dict AS dict, tokens WHERE dict.term = tokens.t),
    qterms AS
        (SELECT termid, docid FROM %fts_schema%.terms AS terms WHERE termid IN (SELECT qtermids.termid FROM qtermids)),
    subscores AS (
        SELECT
            docs.docid, len, term_tf.termid, tf, df,
            (log(((SELECT num_docs FROM %fts_schema%.stats) - df + 0.5) / (df + 0.5))* ((tf * (k + 1)/(tf + k * (1 - b + b * (len / (SELECT avgdl FROM %fts_schema%.stats))))))) AS subscore
        FROM
            (SELECT termid, docid, COUNT(*) AS tf FROM qterms GROUP BY docid, termid) AS term_tf
        JOIN
            (SELECT docid FROM qterms GROUP BY docid HAVING CASE WHEN conjunctive THEN COUNT(DISTINCT termid) = (SELECT COUNT(*) FROM tokens) ELSE 1 END) AS cdocs
        ON
            term_tf.docid = cdocs.docid
        JOIN
            %fts_schema%.docs AS docs
        ON
            term_tf.docid = docs.docid
        JOIN
            %fts_schema%.dict AS dict
        ON
            term_tf.termid = dict.termid
    )
    SELECT name
    FROM (SELECT docid, sum(subscore) AS score FROM subscores GROUP BY docid) AS scores
    JOIN %fts_schema%.docs AS docs
    ON scores.docid = docs.docid ORDER BY score DESC LIMIT 1000
);
