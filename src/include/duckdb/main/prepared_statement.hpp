//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/prepared_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"

namespace duckdb {
class ClientContext;
class PreparedStatementData;

//! A prepared statement
class PreparedStatement {
public:
	//! Create a successfully prepared prepared statement object with the given name
	DUCKDB_API PreparedStatement(shared_ptr<ClientContext> context, shared_ptr<PreparedStatementData> data,
	                             string query, identifier_map_t<idx_t> named_param_map);
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
	identifier_map_t<idx_t> named_param_map;

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
	DUCKDB_API const vector<Identifier> &GetNames();
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
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(identifier_map_t<BoundParameterData> &named_values,
	                                                       bool allow_stream_result = true);

	//! Execute the prepared statement with the given set of values
	DUCKDB_API unique_ptr<QueryResult> Execute(vector<Value> &values, bool allow_stream_result = true);

	//! Execute the prepared statement with the given set of named+unnamed values
	DUCKDB_API unique_ptr<QueryResult> Execute(identifier_map_t<BoundParameterData> &named_values,
	                                           bool allow_stream_result = true);

	//! Execute the prepared statement with the given set of arguments
	template <typename... ARGS>
	unique_ptr<QueryResult> Execute(ARGS... args) {
		vector<Value> values;
		return ExecuteRecursive(values, args...);
	}

	template <class PAYLOAD>
	static string ExcessValuesException(const identifier_map_t<idx_t> &parameters,
	                                    const identifier_map_t<PAYLOAD> &values) {
		// Too many values
		set<string> excess_set;
		for (auto &pair : values) {
			auto &name = pair.first;
			if (!parameters.count(name)) {
				excess_set.insert(name.GetIdentifierName());
			}
		}
		vector<string> excess_values;
		for (auto &val : excess_set) {
			excess_values.push_back(val);
		}
		return StringUtil::Format("Parameter argument/count mismatch, identifiers of the excess parameters: %s",
		                          StringUtil::Join(excess_values, ", "));
	}

	static bool AllowsUserVariableFallback(const Identifier &identifier) {
		auto &name = identifier.GetIdentifierName();
		if (name.empty()) {
			return false;
		}
		return !StringUtil::CharacterIsDigit(name[0]);
	}

	template <class PAYLOAD>
	static string MissingValuesException(const identifier_map_t<idx_t> &parameters,
	                                     const identifier_map_t<PAYLOAD> &values, ClientContext *context = nullptr) {
		// Missing values
		identifier_set_t missing_set;
		for (auto &pair : parameters) {
			auto &name = pair.first;
			if (!values.count(name)) {
				Value variable_value;
				if (context && AllowsUserVariableFallback(name) &&
				    ClientConfig::GetConfig(*context).GetUserVariable(name, variable_value)) {
					continue;
				}
				missing_set.insert(name);
			}
		}
		vector<Identifier> missing_values;
		for (auto &val : missing_set) {
			missing_values.push_back(val);
		}
		return StringUtil::Format("Values were not provided for the following parameters: %s",
		                          StringUtil::Join(missing_values, ", "));
	}

	template <class PAYLOAD>
	static void VerifyParameters(const identifier_map_t<PAYLOAD> &provided, const identifier_map_t<idx_t> &expected,
	                             ClientContext *context = nullptr) {
		for (auto &pair : provided) {
			if (!expected.count(pair.first)) {
				throw InvalidInputException(ExcessValuesException(expected, provided));
			}
		}
		for (auto &pair : expected) {
			auto &identifier = pair.first;
			if (provided.count(identifier)) {
				continue;
			}
			Value variable_value;
			if (context && AllowsUserVariableFallback(identifier) &&
			    ClientConfig::GetConfig(*context).GetUserVariable(identifier, variable_value)) {
				continue;
			}
			throw InvalidInputException(MissingValuesException(expected, provided, context));
		}
	}

	//! Returns whether or not we can / want to cache a logical plan
	static bool CanCachePlan(const LogicalOperator &op);

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
