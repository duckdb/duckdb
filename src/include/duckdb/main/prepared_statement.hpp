//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/prepared_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
class ClientContext;
class PreparedStatementData;

//! A prepared statement
class PreparedStatement {
public:
	//! Create a successfully prepared prepared statement object with the given name
	DUCKDB_API PreparedStatement(shared_ptr<ClientContext> context, shared_ptr<PreparedStatementData> data,
	                             string query, idx_t n_param, case_insensitive_map_t<idx_t> named_param_map);
	//! Create a prepared statement that was not successfully prepared
	DUCKDB_API explicit PreparedStatement(PreservedError error);

	DUCKDB_API ~PreparedStatement();

public:
	//! The client context this prepared statement belongs to
	shared_ptr<ClientContext> context;
	//! The prepared statement data
	shared_ptr<PreparedStatementData> data;
	//! The query that is being prepared
	string query;
	//! Whether or not the statement was successfully prepared
	bool success;
	//! The error message (if success = false)
	PreservedError error;
	//! The amount of bound parameters
	idx_t n_param;
	//! The (optional) named parameters
	case_insensitive_map_t<idx_t> named_param_map;

public:
	//! Gives over ownership of the error object
	DUCKDB_API PreservedError &&TakeErrorObject();
	//! Returns the stored error message
	DUCKDB_API const string &GetError();
	//! Returns whether or not an error occurred
	DUCKDB_API bool HasError() const;
	//! Returns the number of columns in the result
	DUCKDB_API idx_t ColumnCount();
	//! Returns the statement type of the underlying prepared statement object
	DUCKDB_API StatementType GetStatementType();
	//! Returns the underlying statement properties
	DUCKDB_API StatementProperties GetStatementProperties();
	//! Returns the result SQL types of the prepared statement
	DUCKDB_API const vector<LogicalType> &GetTypes();
	//! Returns the result names of the prepared statement
	DUCKDB_API const vector<string> &GetNames();

	//! Create a pending query result of the prepared statement with the given set of arguments
	template <typename... Args>
	unique_ptr<PendingQueryResult> PendingQuery(Args... args) {
		vector<Value> values;
		return PendingQueryRecursive(values, args...);
	}

	//! Execute the prepared statement with the given set of arguments
	template <typename... Args>
	unique_ptr<QueryResult> Execute(Args... args) {
		vector<Value> values;
		return ExecuteRecursive(values, args...);
	}

	//! Create a pending query result of the prepared statement with the given set of arguments
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(vector<Value> &values, bool allow_stream_result = true);

	//! Execute the prepared statement with the given set of values
	DUCKDB_API unique_ptr<QueryResult> Execute(vector<Value> &values, bool allow_stream_result = true);

private:
	unique_ptr<PendingQueryResult> PendingQueryRecursive(vector<Value> &values) {
		return PendingQuery(values);
	}

	template <typename T, typename... Args>
	unique_ptr<PendingQueryResult> PendingQueryRecursive(vector<Value> &values, T value, Args... args) {
		values.push_back(Value::CreateValue<T>(value));
		return PendingQueryRecursive(values, args...);
	}

	unique_ptr<QueryResult> ExecuteRecursive(vector<Value> &values) {
		return Execute(values);
	}

	template <typename T, typename... Args>
	unique_ptr<QueryResult> ExecuteRecursive(vector<Value> &values, T value, Args... args) {
		values.push_back(Value::CreateValue<T>(value));
		return ExecuteRecursive(values, args...);
	}
};

} // namespace duckdb
