#include "fts_indexing.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

#define SQL(...) #__VA_ARGS__

static string fts_schema_name(string schema, string table) {
    return "fts_" + schema + "_" + table;
}

static void drop_fts_index_function(ClientContext &context, bool force) {
    string schema;
    string table;

    string fts_schema = fts_schema_name(schema, table);
    if (!context.catalog.schemas->GetEntry(context, fts_schema))
        throw Exception("a FTS index does not exist on table " + schema + "." + table + ". Create one first with create_fts_index.");

    string cascade = force ? " CASCADE" : "";

    context.transaction.BeginTransaction();
	context.Query("DROP SCHEMA " + fts_schema + cascade + ";", true);
    context.transaction.Commit();
}

static string indexing_script(string input_schema, string input_table, string input_id, string input_val) {
    string fts_schema = fts_schema_name(input_schema, input_table);
    // weird way to have decently readable SQL code in here
    string result = SQL(
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
    );

    // fill in variables (inefficiently)
    result = StringUtil::Replace(result, "%fts_schema%", fts_schema);
    result = StringUtil::Replace(result, "%input_schema%", input_schema);
    result = StringUtil::Replace(result, "%input_table%", input_table);
    result = StringUtil::Replace(result, "%input_id%", input_id);
    result = StringUtil::Replace(result, "%input_val%", input_val);

    return result;
}

static void create_fts_index_function(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state, DataChunk &output) {
    // just gonna act as if we have these
    string schema;
    string table;
    string docid;
    string docval;
    string stemmer;

    string fts_schema = fts_schema_name(schema, table);
    if (context.catalog.schemas->GetEntry(context, fts_schema))
        throw Exception("a FTS index already exists on table " + schema + "." + table + ". Drop it with drop_fts_index before creating a new one.");
    
	context.transaction.BeginTransaction();
    context.Query(indexing_script(schema, table, docid, docval), true);
    context.transaction.Commit();
}

} // namespace duckdb