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
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"

namespace duckdb {
class ClientContext;
class PreparedStatementData;

//! A prepared statement
class PreparedStatement {
public:
	//! Create a successfully prepared prepared statement object with the given name
	DUCKDB_API PreparedStatement(shared_ptr<ClientContext> context, shared_ptr<PreparedStatementData> data,
	                             string query, case_insensitive_map_t<idx_t> named_param_map);
	//! Create a prepared statement that was not successfully prepared
	DUCKDB_API explicit PreparedStatement(ErrorData error);

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
	ErrorData error;
	//! The parameter mapping
	case_insensitive_map_t<idx_t> named_param_map;

public:
	//! Returns the stored error message
	DUCKDB_API const string &GetError();
	//! Returns the stored error object
	DUCKDB_API ErrorData &GetErrorObject();
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
	//! Returns the map of parameter index to the expected type of parameter
	DUCKDB_API case_insensitive_map_t<LogicalType> GetExpectedParameterTypes() const;

	//! Create a pending query result of the prepared statement with the given set of arguments
	template <typename... ARGS>
	unique_ptr<PendingQueryResult> PendingQuery(ARGS... args) {
		vector<Value> values;
		return PendingQueryRecursive(values, args...);
	}

	//! Create a pending query result of the prepared statement with the given set of arguments
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(vector<Value> &values, bool allow_stream_result = true);

	//! Create a pending query result of the prepared statement with the given set named arguments
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(case_insensitive_map_t<BoundParameterData> &named_values,
	                                                       bool allow_stream_result = true);

	//! Execute the prepared statement with the given set of values
	DUCKDB_API unique_ptr<QueryResult> Execute(vector<Value> &values, bool allow_stream_result = true);

	//! Execute the prepared statement with the given set of named+unnamed values
	DUCKDB_API unique_ptr<QueryResult> Execute(case_insensitive_map_t<BoundParameterData> &named_values,
	                                           bool allow_stream_result = true);

	//! Execute the prepared statement with the given set of arguments
	template <typename... ARGS>
	unique_ptr<QueryResult> Execute(ARGS... args) {
		vector<Value> values;
		return ExecuteRecursive(values, args...);
	}

	template <class PAYLOAD>
	static string ExcessValuesException(const case_insensitive_map_t<idx_t> &parameters,
	                                    case_insensitive_map_t<PAYLOAD> &values) {
		// Too many values
		set<string> excess_set;
		for (auto &pair : values) {
			auto &name = pair.first;
			if (!parameters.count(name)) {
				excess_set.insert(name);
			}
		}
		vector<string> excess_values;
		for (auto &val : excess_set) {
			excess_values.push_back(val);
		}
		return StringUtil::Format("Parameter argument/count mismatch, identifiers of the excess parameters: %s",
		                          StringUtil::Join(excess_values, ", "));
	}

	template <class PAYLOAD>
	static string MissingValuesException(const case_insensitive_map_t<idx_t> &parameters,
	                                     case_insensitive_map_t<PAYLOAD> &values) {
		// Missing values
		set<string> missing_set;
		for (auto &pair : parameters) {
			auto &name = pair.first;
			if (!values.count(name)) {
				missing_set.insert(name);
			}
		}
		vector<string> missing_values;
		for (auto &val : missing_set) {
			missing_values.push_back(val);
		}
		return StringUtil::Format("Values were not provided for the following prepared statement parameters: %s",
		                          StringUtil::Join(missing_values, ", "));
	}

	template <class PAYLOAD>
	static void VerifyParameters(case_insensitive_map_t<PAYLOAD> &provided,
	                             const case_insensitive_map_t<idx_t> &expected) {
		if (expected.size() == provided.size()) {
			// Same amount of identifiers, if
			for (auto &pair : expected) {
				auto &identifier = pair.first;
				if (!provided.count(identifier)) {
					throw InvalidInputException(MissingValuesException(expected, provided));
				}
			}
			return;
		}
		// Mismatch in expected and provided parameters/values
		if (expected.size() > provided.size()) {
			throw InvalidInputException(MissingValuesException(expected, provided));
		} else {
			D_ASSERT(provided.size() > expected.size());
			throw InvalidInputException(ExcessValuesException(expected, provided));
		}
	}

private:
	unique_ptr<PendingQueryResult> PendingQueryRecursive(vector<Value> &values) {
		return PendingQuery(values);
	}

	template <typename T, typename... ARGS>
	unique_ptr<PendingQueryResult> PendingQueryRecursive(vector<Value> &values, T value, ARGS... args) {
		values.push_back(Value::CreateValue<T>(value));
		return PendingQueryRecursive(values, args...);
	}

	unique_ptr<QueryResult> ExecuteRecursive(vector<Value> &values) {
		return Execute(values);
	}

	template <typename T, typename... ARGS>
	unique_ptr<QueryResult> ExecuteRecursive(vector<Value> &values, T value, ARGS... args) {
		values.push_back(Value::CreateValue<T>(value));
		return ExecuteRecursive(values, args...);
	}
};

} // namespace duckdb
