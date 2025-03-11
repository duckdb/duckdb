//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/function/udf_function.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/main/profiling_node.hpp"

namespace duckdb {

class ColumnDataCollection;
class ClientContext;

class DatabaseInstance;
class DuckDB;
class LogicalOperator;
class SelectStatement;
struct CSVReaderOptions;

typedef void (*warning_callback_t)(std::string);

//! A connection to a database. This represents a (client) connection that can
//! be used to query the database.
class Connection {
public:
	DUCKDB_API explicit Connection(DuckDB &database);
	DUCKDB_API explicit Connection(DatabaseInstance &database);
	// disable copy constructors
	Connection(const Connection &other) = delete;
	Connection &operator=(const Connection &) = delete;
	//! enable move constructors
	DUCKDB_API Connection(Connection &&other) noexcept;
	DUCKDB_API Connection &operator=(Connection &&) noexcept;
	DUCKDB_API ~Connection();

	shared_ptr<ClientContext> context;
	warning_callback_t warning_cb;

public:
	//! Returns query profiling information for the current query
	DUCKDB_API string GetProfilingInformation(ProfilerPrintFormat format = ProfilerPrintFormat::QUERY_TREE);

	//! Returns the first node of the query profiling tree
	DUCKDB_API optional_ptr<ProfilingNode> GetProfilingTree();

	//! Interrupt execution of the current query
	DUCKDB_API void Interrupt();

	//! Enable query profiling
	DUCKDB_API void EnableProfiling();
	//! Disable query profiling
	DUCKDB_API void DisableProfiling();

	//! Enable aggressive verification/testing of queries, should only be used in testing
	DUCKDB_API void EnableQueryVerification();
	DUCKDB_API void DisableQueryVerification();
	//! Force parallel execution, even for smaller tables. Should only be used in testing.
	DUCKDB_API void ForceParallelism();

	//! Issues a query to the database and returns a QueryResult. This result can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The result can be stepped through with calls to Fetch(). Note that there can only be
	//! one active StreamQueryResult per Connection object. Calling SendQuery() will invalidate any previously existing
	//! StreamQueryResult.
	DUCKDB_API unique_ptr<QueryResult> SendQuery(const string &query);
	//! Issues a query to the database and materializes the result (if necessary). Always returns a
	//! MaterializedQueryResult.
	DUCKDB_API unique_ptr<MaterializedQueryResult> Query(const string &query);
	//! Issues a query to the database and materializes the result (if necessary). Always returns a
	//! MaterializedQueryResult.
	DUCKDB_API unique_ptr<MaterializedQueryResult> Query(unique_ptr<SQLStatement> statement);
	// prepared statements
	template <typename... ARGS>
	unique_ptr<QueryResult> Query(const string &query, ARGS... args) {
		vector<Value> values;
		return QueryParamsRecursive(query, values, args...);
	}

	//! Issues a query to the database and returns a Pending Query Result. Note that "query" may only contain
	//! a single statement.
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(const string &query, bool allow_stream_result = false);
	//! Issues a query to the database and returns a Pending Query Result
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(unique_ptr<SQLStatement> statement,
	                                                       bool allow_stream_result = false);
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(unique_ptr<SQLStatement> statement,
	                                                       case_insensitive_map_t<BoundParameterData> &named_values,
	                                                       bool allow_stream_result = false);
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(const string &query,
	                                                       case_insensitive_map_t<BoundParameterData> &named_values,
	                                                       bool allow_stream_result = false);
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(const string &query, vector<Value> &values,
	                                                       bool allow_stream_result = false);
	DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(unique_ptr<SQLStatement> statement, vector<Value> &values,
	                                                       bool allow_stream_result = false);

	//! Prepare the specified query, returning a prepared statement object
	DUCKDB_API unique_ptr<PreparedStatement> Prepare(const string &query);
	//! Prepare the specified statement, returning a prepared statement object
	DUCKDB_API unique_ptr<PreparedStatement> Prepare(unique_ptr<SQLStatement> statement);

	//! Get the table info of a specific table, or nullptr if it cannot be found.
	DUCKDB_API unique_ptr<TableDescription> TableInfo(const string &database_name, const string &schema_name,
	                                                  const string &table_name);
	//! Get the table info of a specific table, or nullptr if it cannot be found. Uses INVALID_CATALOG.
	DUCKDB_API unique_ptr<TableDescription> TableInfo(const string &schema_name, const string &table_name);
	//! Get the table info of a specific table, or nullptr if it cannot be found. Uses INVALID_CATALOG and
	//! DEFAULT_SCHEMA.
	DUCKDB_API unique_ptr<TableDescription> TableInfo(const string &table_name);

	//! Extract a set of SQL statements from a specific query
	DUCKDB_API vector<unique_ptr<SQLStatement>> ExtractStatements(const string &query);
	//! Extract the logical plan that corresponds to a query
	DUCKDB_API unique_ptr<LogicalOperator> ExtractPlan(const string &query);

	//! Appends a DataChunk to the specified table
	DUCKDB_API void Append(TableDescription &description, DataChunk &chunk);
	//! Appends a ColumnDataCollection to the specified table
	DUCKDB_API void Append(TableDescription &description, ColumnDataCollection &collection);

	//! Returns a relation that produces a table from this connection
	DUCKDB_API shared_ptr<Relation> Table(const string &tname);
	DUCKDB_API shared_ptr<Relation> Table(const string &schema_name, const string &table_name);
	DUCKDB_API shared_ptr<Relation> Table(const string &catalog_name, const string &schema_name,
	                                      const string &table_name);
	//! Returns a relation that produces a view from this connection
	DUCKDB_API shared_ptr<Relation> View(const string &tname);
	DUCKDB_API shared_ptr<Relation> View(const string &schema_name, const string &table_name);
	//! Returns a relation that calls a specified table function
	DUCKDB_API shared_ptr<Relation> TableFunction(const string &tname);
	DUCKDB_API shared_ptr<Relation> TableFunction(const string &tname, const vector<Value> &values,
	                                              const named_parameter_map_t &named_parameters);
	DUCKDB_API shared_ptr<Relation> TableFunction(const string &tname, const vector<Value> &values);
	//! Returns a relation that produces values
	DUCKDB_API shared_ptr<Relation> Values(const vector<vector<Value>> &values);
	DUCKDB_API shared_ptr<Relation> Values(vector<vector<unique_ptr<ParsedExpression>>> &&values);
	DUCKDB_API shared_ptr<Relation> Values(const vector<vector<Value>> &values, const vector<string> &column_names,
	                                       const string &alias = "values");
	DUCKDB_API shared_ptr<Relation> Values(const string &values);
	DUCKDB_API shared_ptr<Relation> Values(const string &values, const vector<string> &column_names,
	                                       const string &alias = "values");

	//! Reads CSV file
	DUCKDB_API shared_ptr<Relation> ReadCSV(const string &csv_file);
	DUCKDB_API shared_ptr<Relation> ReadCSV(const vector<string> &csv_input, named_parameter_map_t &&options);
	DUCKDB_API shared_ptr<Relation> ReadCSV(const string &csv_input, named_parameter_map_t &&options);
	DUCKDB_API shared_ptr<Relation> ReadCSV(const string &csv_file, const vector<string> &columns);

	//! Reads Parquet file
	DUCKDB_API shared_ptr<Relation> ReadParquet(const string &parquet_file, bool binary_as_string);
	//! Returns a relation from a query
	DUCKDB_API shared_ptr<Relation> RelationFromQuery(const string &query, const string &alias = "queryrelation",
	                                                  const string &error = "Expected a single SELECT statement");
	DUCKDB_API shared_ptr<Relation> RelationFromQuery(unique_ptr<SelectStatement> select_stmt,
	                                                  const string &alias = "queryrelation", const string &query = "");

	DUCKDB_API void BeginTransaction();
	DUCKDB_API void Commit();
	DUCKDB_API void Rollback();
	DUCKDB_API void SetAutoCommit(bool auto_commit);
	DUCKDB_API bool IsAutoCommit();
	DUCKDB_API bool HasActiveTransaction();

	//! Fetch a list of table names that are required for a given query
	DUCKDB_API unordered_set<string> GetTableNames(const string &query);

	// NOLINTBEGIN
	template <typename TR, typename... ARGS>
	void CreateScalarFunction(const string &name, TR (*udf_func)(ARGS...)) {
		scalar_function_t function = UDFWrapper::CreateScalarFunction<TR, ARGS...>(name, udf_func);
		UDFWrapper::RegisterFunction<TR, ARGS...>(name, function, *context);
	}

	template <typename TR, typename... ARGS>
	void CreateScalarFunction(const string &name, vector<LogicalType> args, LogicalType ret_type,
	                          TR (*udf_func)(ARGS...)) {
		scalar_function_t function = UDFWrapper::CreateScalarFunction<TR, ARGS...>(name, args, ret_type, udf_func);
		UDFWrapper::RegisterFunction(name, args, ret_type, function, *context);
	}

	template <typename TR, typename... ARGS>
	void CreateVectorizedFunction(const string &name, scalar_function_t udf_func,
	                              LogicalType varargs = LogicalType::INVALID) {
		UDFWrapper::RegisterFunction<TR, ARGS...>(name, udf_func, *context, std::move(varargs));
	}

	void CreateVectorizedFunction(const string &name, vector<LogicalType> args, LogicalType ret_type,
	                              scalar_function_t udf_func, LogicalType varargs = LogicalType::INVALID) {
		UDFWrapper::RegisterFunction(name, std::move(args), std::move(ret_type), std::move(udf_func), *context,
		                             std::move(varargs));
	}

	//------------------------------------- Aggreate Functions ----------------------------------------//
	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	void CreateAggregateFunction(const string &name) {
		AggregateFunction function = UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA>(name);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	void CreateAggregateFunction(const string &name) {
		AggregateFunction function = UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	void CreateAggregateFunction(const string &name, LogicalType ret_type, LogicalType input_type_a) {
		AggregateFunction function =
		    UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA>(name, ret_type, input_type_a);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	void CreateAggregateFunction(const string &name, LogicalType ret_type, LogicalType input_type_a,
	                             LogicalType input_type_b) {
		AggregateFunction function =
		    UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name, ret_type, input_type_a, input_type_b);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	void CreateAggregateFunction(const string &name, const vector<LogicalType> &arguments,
	                             const LogicalType &return_type, aggregate_size_t state_size,
	                             aggregate_initialize_t initialize, aggregate_update_t update,
	                             aggregate_combine_t combine, aggregate_finalize_t finalize,
	                             aggregate_simple_update_t simple_update = nullptr,
	                             bind_aggregate_function_t bind = nullptr,
	                             aggregate_destructor_t destructor = nullptr) {
		AggregateFunction function =
		    UDFWrapper::CreateAggregateFunction(name, arguments, return_type, state_size, initialize, update, combine,
		                                        finalize, simple_update, bind, destructor);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}
	// NOLINTEND

protected:
	//! Identified used to uniquely identify connections to the database.
	connection_t connection_id;

private:
	unique_ptr<QueryResult> QueryParamsRecursive(const string &query, vector<Value> &values);

	template <typename T, typename... ARGS>
	unique_ptr<QueryResult> QueryParamsRecursive(const string &query, vector<Value> &values, T value, ARGS... args) {
		values.push_back(Value::CreateValue<T>(value));
		return QueryParamsRecursive(query, values, args...);
	}
};

} // namespace duckdb
