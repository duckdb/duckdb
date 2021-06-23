//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/progress_bar.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include <random>
#include "duckdb/common/atomic.hpp"

namespace duckdb {
class Appender;
class Catalog;
class DatabaseInstance;
class LogicalOperator;
class PreparedStatementData;
class Relation;
class BufferedFileWriter;
class QueryProfiler;
class QueryProfilerHistory;
class ClientContextLock;
struct CreateScalarFunctionInfo;
class ScalarFunctionCatalogEntry;

//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext : public std::enable_shared_from_this<ClientContext> {
	friend class TransactionManager;

public:
	DUCKDB_API explicit ClientContext(shared_ptr<DatabaseInstance> db);
	DUCKDB_API ~ClientContext();
	//! Query profiler
	unique_ptr<QueryProfiler> profiler;
	//! QueryProfiler History
	unique_ptr<QueryProfilerHistory> query_profiler_history;
	//! The database that this client is connected to
	shared_ptr<DatabaseInstance> db;
	//! Data for the currently running transaction
	TransactionContext transaction;
	//! Whether or not the query is interrupted
	atomic<bool> interrupted;
	//! The current query being executed by the client context
	string query;

	//! The query executor
	Executor executor;

	//! The Progress Bar
	unique_ptr<ProgressBar> progress_bar;
	//! If the progress bar is enabled or not.
	bool enable_progress_bar = false;
	//! If the print of the progress bar is enabled
	bool print_progress_bar = true;
	//! The wait time before showing the progress bar
	int wait_time = 2000;

	unique_ptr<SchemaCatalogEntry> temporary_objects;
	unordered_map<string, shared_ptr<PreparedStatementData>> prepared_statements;

	// Whether or not aggressive query verification is enabled
	bool query_verification_enabled = false;
	//! Enable the running of optimizers
	bool enable_optimizer = true;
	//! Force parallelism of small tables, used for testing
	bool force_parallelism = false;
	//! Force index join independent of table cardinality, used for testing
	bool force_index_join = false;
	//! Maximum bits allowed for using a perfect hash table (i.e. the perfect HT can hold up to 2^perfect_ht_threshold
	//! elements)
	idx_t perfect_ht_threshold = 12;
	//! The writer used to log queries (if logging is enabled)
	unique_ptr<BufferedFileWriter> log_query_writer;
	//! The explain output type used when none is specified (default: PHYSICAL_ONLY)
	ExplainOutputType explain_output_type = ExplainOutputType::PHYSICAL_ONLY;
	//! The random generator used by random(). Its seed value can be set by setseed().
	std::mt19937 random_engine;

	//! The schema search path, in order by which entries are searched if no schema entry is provided
	vector<string> catalog_search_path = {TEMP_SCHEMA, DEFAULT_SCHEMA, "pg_catalog"};

public:
	DUCKDB_API Transaction &ActiveTransaction() {
		return transaction.ActiveTransaction();
	}

	//! Interrupt execution of a query
	DUCKDB_API void Interrupt();
	//! Enable query profiling
	DUCKDB_API void EnableProfiling();
	//! Disable query profiling
	DUCKDB_API void DisableProfiling();

	//! Issue a query, returning a QueryResult. The QueryResult can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The StreamQueryResult will only be returned in the case of a successful SELECT
	//! statement.
	DUCKDB_API unique_ptr<QueryResult> Query(const string &query, bool allow_stream_result);
	DUCKDB_API unique_ptr<QueryResult> Query(unique_ptr<SQLStatement> statement, bool allow_stream_result);
	//! Fetch a query from the current result set (if any)
	DUCKDB_API unique_ptr<DataChunk> Fetch();
	//! Cleanup the result set (if any).
	DUCKDB_API void Cleanup();
	//! Destroy the client context
	DUCKDB_API void Destroy();

	//! Get the table info of a specific table, or nullptr if it cannot be found
	DUCKDB_API unique_ptr<TableDescription> TableInfo(const string &schema_name, const string &table_name);
	//! Appends a DataChunk to the specified table. Returns whether or not the append was successful.
	DUCKDB_API void Append(TableDescription &description, DataChunk &chunk);
	//! Try to bind a relation in the current client context; either throws an exception or fills the result_columns
	//! list with the set of returned columns
	DUCKDB_API void TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns);

	//! Execute a relation
	DUCKDB_API unique_ptr<QueryResult> Execute(const shared_ptr<Relation> &relation);

	//! Prepare a query
	DUCKDB_API unique_ptr<PreparedStatement> Prepare(const string &query);
	//! Directly prepare a SQL statement
	DUCKDB_API unique_ptr<PreparedStatement> Prepare(unique_ptr<SQLStatement> statement);

	//! Execute a prepared statement with the given name and set of parameters
	//! It is possible that the prepared statement will be re-bound. This will generally happen if the catalog is
	//! modified in between the prepared statement being bound and the prepared statement being run.
	DUCKDB_API unique_ptr<QueryResult> Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
	                                           vector<Value> &values, bool allow_stream_result = true);

	//! Gets current percentage of the query's progress, returns 0 in case the progress bar is disabled.
	int GetProgress();

	//! Register function in the temporary schema
	DUCKDB_API void RegisterFunction(CreateFunctionInfo *info);

	//! Parse statements from a query
	DUCKDB_API vector<unique_ptr<SQLStatement>> ParseStatements(const string &query);
	//! Extract the logical plan of a query
	DUCKDB_API unique_ptr<LogicalOperator> ExtractPlan(const string &query);
	void HandlePragmaStatements(vector<unique_ptr<SQLStatement>> &statements);

	//! Runs a function with a valid transaction context, potentially starting a transaction if the context is in auto
	//! commit mode.
	DUCKDB_API void RunFunctionInTransaction(const std::function<void(void)> &fun,
	                                         bool requires_valid_transaction = true);
	//! Same as RunFunctionInTransaction, but does not obtain a lock on the client context or check for validation
	DUCKDB_API void RunFunctionInTransactionInternal(ClientContextLock &lock, const std::function<void(void)> &fun,
	                                                 bool requires_valid_transaction = true);

private:
	//! Parse statements from a query
	vector<unique_ptr<SQLStatement>> ParseStatementsInternal(ClientContextLock &lock, const string &query);
	//! Perform aggressive query verification of a SELECT statement. Only called when query_verification_enabled is
	//! true.
	string VerifyQuery(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement);

	void InitialCleanup(ClientContextLock &lock);
	//! Internal clean up, does not lock. Caller must hold the context_lock.
	void CleanupInternal(ClientContextLock &lock);
	string FinalizeQuery(ClientContextLock &lock, bool success);
	//! Internal fetch, does not lock. Caller must hold the context_lock.
	unique_ptr<DataChunk> FetchInternal(ClientContextLock &lock);
	//! Internally execute a set of SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> RunStatements(ClientContextLock &lock, const string &query,
	                                      vector<unique_ptr<SQLStatement>> &statements, bool allow_stream_result);
	//! Internally prepare and execute a prepared SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> RunStatement(ClientContextLock &lock, const string &query,
	                                     unique_ptr<SQLStatement> statement, bool allow_stream_result);
	unique_ptr<QueryResult> RunStatementOrPreparedStatement(ClientContextLock &lock, const string &query,
	                                                        unique_ptr<SQLStatement> statement,
	                                                        shared_ptr<PreparedStatementData> &prepared,
	                                                        vector<Value> *values, bool allow_stream_result);

	//! Internally prepare a SQL statement. Caller must hold the context_lock.
	shared_ptr<PreparedStatementData> CreatePreparedStatement(ClientContextLock &lock, const string &query,
	                                                          unique_ptr<SQLStatement> statement);
	//! Internally execute a prepared SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> ExecutePreparedStatement(ClientContextLock &lock, const string &query,
	                                                 shared_ptr<PreparedStatementData> statement,
	                                                 vector<Value> bound_values, bool allow_stream_result);
	//! Call CreatePreparedStatement() and ExecutePreparedStatement() without any bound values
	unique_ptr<QueryResult> RunStatementInternal(ClientContextLock &lock, const string &query,
	                                             unique_ptr<SQLStatement> statement, bool allow_stream_result);
	unique_ptr<PreparedStatement> PrepareInternal(ClientContextLock &lock, unique_ptr<SQLStatement> statement);
	void LogQueryInternal(ClientContextLock &lock, const string &query);

	unique_ptr<ClientContextLock> LockContext();

	bool UpdateFunctionInfoFromEntry(ScalarFunctionCatalogEntry *existing_function, CreateScalarFunctionInfo *new_info);

private:
	//! The currently opened StreamQueryResult (if any)
	StreamQueryResult *open_result = nullptr;
	//! Lock on using the ClientContext in parallel
	mutex context_lock;
};

} // namespace duckdb
