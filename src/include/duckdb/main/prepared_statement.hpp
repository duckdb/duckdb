//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/prepared_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {
class ClientContext;
class PreparedStatementData;

//! A prepared statement
class PreparedStatement {
public:
	//! Create a successfully prepared prepared statement object with the given name
	PreparedStatement(ClientContext *context, string name, string query, PreparedStatementData &data,
	                  idx_t n_param = 0);
	//! Create a prepared statement that was not successfully prepared
	PreparedStatement(string error);

	~PreparedStatement();

public:
	StatementType type;
	//! The client context this prepared statement belongs to
	ClientContext *context;
	//! The internal name of the prepared statement
	string name;
	//! The query that is being prepared
	string query;
	//! Whether or not the statement was successfully prepared
	bool success;
	//! The error message (if success = false)
	string error;
	//! Whether or not the prepared statement has been invalidated because the underlying connection has been destroyed
	bool is_invalidated;
	//! The amount of bound parameters
	idx_t n_param;
	//! The result SQL types of the prepared statement
	vector<SQLType> types;
	//! The result names of the prepared statement
	vector<string> names;

public:
	//! Execute the prepared statement with the given set of arguments
	template <typename... Args> unique_ptr<QueryResult> Execute(Args... args) {
		vector<Value> values;
		return ExecuteRecursive(values, args...);
	}

	//! Execute the prepared statement with the given set of values
	unique_ptr<QueryResult> Execute(vector<Value> &values, bool allow_stream_result = true);

private:
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
