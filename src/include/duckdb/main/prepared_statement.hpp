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
class SQLStatement;

//! The metadata of a prepared statement, as it was prepared
struct PreparedStatementInfo {
	//! The result names of the prepared statement
	vector<Identifier> names;
	//! The result types of the prepared statement
	vector<LogicalType> types;
	//! The type of the statement that was prepared
	StatementType statement_type = StatementType::INVALID_STATEMENT;
	//! The properties of the statement that was prepared
	StatementProperties properties;
	//! The mapping of parameter identifier to parameter index
	identifier_map_t<idx_t> named_param_map;
	//! The expected type of each parameter whose type could be resolved when preparing
	identifier_map_t<LogicalType> parameter_types;
};

//! A handle to a prepared statement that lives in the client context. The statement itself is prepared through
//! `PREPARE <name> AS ...` and executed through `EXECUTE <name>(...)` - this class only holds the name.
class PreparedStatement {
public:
	//! Create a handle to the prepared statement with the given name in the client context
	DUCKDB_API PreparedStatement(const shared_ptr<ClientContext> &context, string name, string query,
	                             PreparedStatementInfo info);
	//! Create a prepared statement that was not successfully prepared
	DUCKDB_API explicit PreparedStatement(ErrorData error);

	DUCKDB_API ~PreparedStatement();

	//! Destroying this object deallocates the prepared statement - so it cannot be copied
	PreparedStatement(const PreparedStatement &) = delete;
	PreparedStatement &operator=(const PreparedStatement &) = delete;

public:
	//! Returns the stored error message
	DUCKDB_API const string &GetError();
	//! Returns the stored error object
	DUCKDB_API ErrorData &GetErrorObject();
	//! Returns whether or not an error occurred
	DUCKDB_API bool HasError() const;
	//! Returns the client context this statement was prepared in - or nullptr if it has been destroyed
	DUCKDB_API shared_ptr<ClientContext> TryGetContext() const;
	//! Returns the name of the prepared statement within the client context
	DUCKDB_API const string &GetName() const;
	//! Returns the query that was prepared
	DUCKDB_API const string &GetQuery() const;
	//! Returns the number of columns in the result
	DUCKDB_API idx_t ColumnCount() const;
	//! Returns the statement type of the underlying prepared statement object
	DUCKDB_API StatementType GetStatementType() const;
	//! Returns the underlying statement properties
	DUCKDB_API const StatementProperties &GetStatementProperties() const;
	//! Returns the result SQL types of the prepared statement
	DUCKDB_API const vector<LogicalType> &GetTypes() const;
	//! Returns the result names of the prepared statement
	DUCKDB_API const vector<Identifier> &GetNames() const;
	//! Returns the mapping of parameter identifier to parameter index
	DUCKDB_API const identifier_map_t<idx_t> &GetNamedParameterMap() const;
	//! Returns the number of parameters of the prepared statement
	DUCKDB_API idx_t GetParameterCount() const;
	//! Try to get the expected type of the parameter with the given identifier
	DUCKDB_API bool TryGetParameterType(const Identifier &identifier, LogicalType &result) const;
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

private:
	//! The client context this prepared statement belongs to
	weak_ptr<ClientContext> context;
	//! The name of the prepared statement within the client context
	string name;
	//! The query that is being prepared
	string query;
	//! Whether or not the statement was successfully prepared
	bool success;
	//! The error message (if success = false)
	ErrorData error;
	//! The metadata of the statement, as it was prepared
	PreparedStatementInfo info;

private:
	//! Create the `EXECUTE <name>(...)` statement that runs this prepared statement with the given values
	unique_ptr<SQLStatement> CreateExecuteStatement(const identifier_map_t<BoundParameterData> &named_values) const;

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
