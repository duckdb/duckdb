//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "duckdb/common/unordered_set.hpp"
#include <random>

namespace duckdb {
class Appender;
class Catalog;
class DuckDB;
class PreparedStatementData;
class Relation;

//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext {
public:
	ClientContext(DuckDB &database);

	//! Query profiler
	QueryProfiler profiler;
	//! The database that this client is connected to
	DuckDB &db;
	//! Data for the currently running transaction
	TransactionContext transaction;
	//! Whether or not the query is interrupted
	bool interrupted;
	//! Whether or not the ClientContext has been invalidated because the underlying database is destroyed
	bool is_invalidated = false;
	//! Lock on using the ClientContext in parallel
	std::mutex context_lock;

	ExecutionContext execution_context;

	Catalog &catalog;
	unique_ptr<SchemaCatalogEntry> temporary_objects;
	unique_ptr<CatalogSet> prepared_statements;

	// Whether or not aggressive query verification is enabled
	bool query_verification_enabled = false;
	//! Enable the running of optimizers
	bool enable_optimizer = true;

	//! The random generator used by random(). Its seed value can be set by setseed().
	std::mt19937 random_engine;

public:
	Transaction &ActiveTransaction() {
		return transaction.ActiveTransaction();
	}

	//! Interrupt execution of a query
	void Interrupt();
	//! Enable query profiling
	void EnableProfiling();
	//! Disable query profiling
	void DisableProfiling();

	//! Issue a query, returning a QueryResult. The QueryResult can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The StreamQueryResult will only be returned in the case of a successful SELECT
	//! statement.
	unique_ptr<QueryResult> Query(string query, bool allow_stream_result);
	//! Fetch a query from the current result set (if any)
	unique_ptr<DataChunk> Fetch();
	//! Cleanup the result set (if any).
	void Cleanup();
	//! Invalidate the client context. The current query will be interrupted and the client context will be invalidated,
	//! making it impossible for future queries to run.
	void Invalidate();

	//! Get the table info of a specific table, or nullptr if it cannot be found
	unique_ptr<TableDescription> TableInfo(string schema_name, string table_name);
	//! Appends a DataChunk to the specified table. Returns whether or not the append was successful.
	void Append(TableDescription &description, DataChunk &chunk);
	//! Try to bind a relation in the current client context; either throws an exception or fills the result_columns
	//! list with the set of returned columns
	void TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns);

	//! Execute a relation
	unique_ptr<QueryResult> Execute(shared_ptr<Relation> relation);

	//! Prepare a query
	unique_ptr<PreparedStatement> Prepare(string query);
	//! Execute a prepared statement with the given name and set of parameters
	unique_ptr<QueryResult> Execute(string name, vector<Value> &values, bool allow_stream_result = true,
	                                string query = string());
	//! Removes a prepared statement from the set of prepared statements in the client context
	void RemovePreparedStatement(PreparedStatement *statement);

	void RegisterAppender(Appender *appender);
	void RemoveAppender(Appender *appender);

private:
	//! Perform aggressive query verification of a SELECT statement. Only called when query_verification_enabled is
	//! true.
	string VerifyQuery(string query, unique_ptr<SQLStatement> statement);

	void InitialCleanup();
	//! Internal clean up, does not lock. Caller must hold the context_lock.
	void CleanupInternal();
	string FinalizeQuery(bool success);
	//! Internal fetch, does not lock. Caller must hold the context_lock.
	unique_ptr<DataChunk> FetchInternal();
	//! Internally execute a set of SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> RunStatements(const string &query, vector<unique_ptr<SQLStatement>> &statements,
	                                      bool allow_stream_result);
	//! Internally prepare and execute a prepared SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> RunStatement(const string &query, unique_ptr<SQLStatement> statement,
	                                     bool allow_stream_result);

	//! Internally prepare a SQL statement. Caller must hold the context_lock.
	unique_ptr<PreparedStatementData> CreatePreparedStatement(const string &query, unique_ptr<SQLStatement> statement);
	//! Internally execute a prepared SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> ExecutePreparedStatement(const string &query, PreparedStatementData &statement,
	                                                 vector<Value> bound_values, bool allow_stream_result);
	//! Call CreatePreparedStatement() and ExecutePreparedStatement() without any bound values
	unique_ptr<QueryResult> RunStatementInternal(const string &query, unique_ptr<SQLStatement> statement,
	                                             bool allow_stream_result);

	template <class T> void RunFunctionInTransaction(T &&fun);

private:
	idx_t prepare_count = 0;
	//! The currently opened StreamQueryResult (if any)
	StreamQueryResult *open_result = nullptr;
	//! Prepared statement objects that were created using the ClientContext::Prepare method
	unordered_set<PreparedStatement *> prepared_statement_objects;
	//! Appenders that were attached to this client context
	unordered_set<Appender *> appenders;
};
} // namespace duckdb
