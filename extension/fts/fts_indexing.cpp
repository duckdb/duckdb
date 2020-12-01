#include "fts_indexing.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

#define SQL(...) #__VA_ARGS__

static string fts_schema_name(string schema, string table) {
	return "fts_" + schema + "_" + table;
}

string drop_fts_index_query(ClientContext &context, FunctionParameters parameters) {
    auto qname = QualifiedName::Parse(parameters.values[0].str_value);
	qname.schema = qname.schema == INVALID_SCHEMA ? DEFAULT_SCHEMA : qname.schema;
	string fts_schema = fts_schema_name(qname.schema, qname.name);

	if (!context.catalog.schemas->GetEntry(context, fts_schema)) {
		throw CatalogException("a FTS index does not exist on table '%s.%s'. Create one with 'PRAGMA create_fts_index()'.",
		                       qname.schema, qname.name);
	}

	return "DROP SCHEMA " + fts_schema + " CASCADE;";
}

static string indexing_script(string input_schema, string input_table, string input_id, vector<string> input_values,
                              string stemmer) {
	string fts_schema = fts_schema_name(input_schema, input_table);
	// weird way to have decently readable SQL code in here
    string result = SQL(
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
                    %fts_schema%.tokenize(%input_values%) AS term,
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
    );

    // fill in variables (inefficiently, but keeps SQL script readable)
	result = StringUtil::Replace(result, "%fts_schema%", fts_schema);
	result = StringUtil::Replace(result, "%input_schema%", input_schema);
	result = StringUtil::Replace(result, "%input_table%", input_table);
	result = StringUtil::Replace(result, "%input_id%", input_id);
	result = StringUtil::Replace(result, "%input_values%", StringUtil::Join(input_values, " || ' ' || "));
	result = StringUtil::Replace(result, "%stemmer%", stemmer);

	return result;
}

string create_fts_index_query(ClientContext &context, FunctionParameters parameters) {
    auto qname = QualifiedName::Parse(parameters.values[0].str_value);
    qname.schema = qname.schema == INVALID_SCHEMA ? DEFAULT_SCHEMA : qname.schema;
    string fts_schema = fts_schema_name(qname.schema, qname.name);

    if (!context.catalog.schemas->GetEntry(context, qname.schema)) {
        throw CatalogException("No such schema: '%s'", qname.schema);
    }
    auto schema = (SchemaCatalogEntry *)context.catalog.schemas->GetEntry(context, qname.schema);
    if (!schema->tables.GetEntry(context, qname.name)) {
        throw CatalogException("No such table: '%s.%s'", qname.schema, qname.name);
    }

	// get named parameters
	string stemmer = "porter";
	if (parameters.named_parameters.find("stemmer") != parameters.named_parameters.end()) {
		stemmer = parameters.named_parameters["stemmer"].str_value;
	}
	bool overwrite = false;
	if (parameters.named_parameters.find("overwrite") != parameters.named_parameters.end()) {
		overwrite = parameters.named_parameters["overwrite"].value_.boolean;
	}

	// throw error if an index already exists on this table
	if (context.catalog.schemas->GetEntry(context, fts_schema) && !overwrite) {
		throw CatalogException("a FTS index already exists on table '%s.%s'. Supply 'overwite=true' to overwrite, or "
		                       "drop the existing index with 'PRAGMA drop_fts_index()' before creating a new one.",
		                       qname.schema, qname.name);
	}

	// positional parameters (vararg document text fields to be indexed)
	auto doc_id = parameters.values[1].str_value;
	vector<string> doc_values;
	for (idx_t i = 2; i < parameters.values.size(); i++) {
		doc_values.push_back(parameters.values[i].str_value);
	}
	if (doc_values.empty()) {
		throw Exception("at least one column must be supplied for indexing!");
	}

	return indexing_script(qname.schema, qname.name, doc_id, doc_values, stemmer);
}

} // namespace duckdb
