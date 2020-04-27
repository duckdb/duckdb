//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class ClientContext;
class DuckDB;

typedef void (*warning_callback)(std::string);

//! A connection to a database. This represents a (client) connection that can
//! be used to query the database.
class Connection {
public:
	Connection(DuckDB &database);
	~Connection();

	DuckDB &db;
	unique_ptr<ClientContext> context;
	warning_callback warning_cb;

public:
	//! Returns query profiling information for the current query
	string GetProfilingInformation(ProfilerPrintFormat format = ProfilerPrintFormat::QUERY_TREE);

	//! Interrupt execution of the current query
	void Interrupt();

	//! Enable query profiling
	void EnableProfiling();
	//! Disable query profiling
	void DisableProfiling();

	void SetWarningCallback(warning_callback);

	//! Enable aggressive verification/testing of queries, should only be used in testing
	void EnableQueryVerification();
	void DisableQueryVerification();

	//! Issues a query to the database and returns a QueryResult. This result can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The result can be stepped through with calls to Fetch(). Note that there can only be
	//! one active StreamQueryResult per Connection object. Calling SendQuery() will invalidate any previously existing
	//! StreamQueryResult.
	unique_ptr<QueryResult> SendQuery(string query);
	//! Issues a query to the database and materializes the result (if necessary). Always returns a
	//! MaterializedQueryResult.
	unique_ptr<MaterializedQueryResult> Query(string query);
	// prepared statements
	template <typename... Args> unique_ptr<QueryResult> Query(string query, Args... args) {
		vector<Value> values;
		return QueryParamsRecursive(query, values, args...);
	}

	//! Prepare the specified query, returning a prepared statement object
	unique_ptr<PreparedStatement> Prepare(string query);

	//! Get the table info of a specific table (in the default schema), or nullptr if it cannot be found
	unique_ptr<TableDescription> TableInfo(string table_name);
	//! Get the table info of a specific table, or nullptr if it cannot be found
	unique_ptr<TableDescription> TableInfo(string schema_name, string table_name);

	//! Extract a set of SQL statements from a specific query
	vector<unique_ptr<SQLStatement>> ExtractStatements(string query);

	//! Appends a DataChunk to the specified table
	void Append(TableDescription &description, DataChunk &chunk);

	//! Returns a relation that produces a table from this connection
	shared_ptr<Relation> Table(string tname);
	shared_ptr<Relation> Table(string schema_name, string table_name);
	//! Returns a relation that produces a view from this connection
	shared_ptr<Relation> View(string tname);
	shared_ptr<Relation> View(string schema_name, string table_name);
	//! Returns a relation that calls a specified table function
	shared_ptr<Relation> TableFunction(string tname);
	shared_ptr<Relation> TableFunction(string tname, vector<Value> values);
	//! Returns a relation that produces values
	shared_ptr<Relation> Values(vector<vector<Value>> values);
	shared_ptr<Relation> Values(vector<vector<Value>> values, vector<string> column_names, string alias = "values");
	shared_ptr<Relation> Values(string values);
	shared_ptr<Relation> Values(string values, vector<string> column_names, string alias = "values");
	//! Reads CSV file
	shared_ptr<Relation> ReadCSV(string csv_file, vector<string> columns);

	void BeginTransaction();
	void Commit();
	void Rollback();

private:
	unique_ptr<QueryResult> QueryParamsRecursive(string query, vector<Value> &values);

	template <typename T, typename... Args>
	unique_ptr<QueryResult> QueryParamsRecursive(string query, vector<Value> &values, T value, Args... args) {
		values.push_back(Value::CreateValue<T>(value));
		return QueryParamsRecursive(query, values, args...);
	}
};

} // namespace duckdb
