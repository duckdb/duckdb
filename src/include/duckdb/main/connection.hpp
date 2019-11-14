//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/enums/profiler_format.hpp"

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

	//! Issues a query to the database and returns a QueryResult. This result can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The result can be stepped through with calls to Fetch(). Note that there can only be
	//! one active StreamQueryResult per Connection object. Calling SendQuery() will invalidate any previously existing
	//! StreamQueryResult.
	unique_ptr<QueryResult> SendQuery(string query);
	//! Issues a query to the database and materializes the result (if necessary). Always returns a
	//! MaterializedQueryResult.
	unique_ptr<MaterializedQueryResult> Query(string query);

	//! Prepare the specified query, returning a prepared statement object
	unique_ptr<PreparedStatement> Prepare(string query);

	Appender *OpenAppender(string schema_name, string table_name);
	void CloseAppender();

	// prepared statements
	template <typename... Args> unique_ptr<QueryResult> Query(string query, Args... args) {
		vector<Value> values;
		return QueryParamsRecursive(query, values, args...);
	}

public:
	DuckDB &db;
	unique_ptr<ClientContext> context;
	warning_callback warning_cb;

private:
	unique_ptr<Appender> appender = nullptr;

	unique_ptr<QueryResult> QueryParamsRecursive(string query, vector<Value> &values);

	template <typename T, typename... Args>
	unique_ptr<QueryResult> QueryParamsRecursive(string query, vector<Value> &values, T value, Args... args) {
		values.push_back(Value::CreateValue<T>(value));
		return QueryParamsRecursive(query, values, args...);
	}
};

} // namespace duckdb
