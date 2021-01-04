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
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <random>

namespace duckdb {
class Appender;
class Catalog;
class DatabaseInstance;
class PreparedStatementData;
class Relation;
class BufferedFileWriter;

//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext : public std::enable_shared_from_this<ClientContext> {
public:
	ClientContext(shared_ptr<DatabaseInstance> db);
	~ClientContext();

	//! Query profiler
	QueryProfiler profiler;
	//! The database that this client is connected to
	shared_ptr<DatabaseInstance> db;
	//! Data for the currently running transaction
	TransactionContext transaction;
	//! Whether or not the query is interrupted
	bool interrupted;
	//! Lock on using the ClientContext in parallel
	std::mutex context_lock;
	//! The current query being executed by the client context
	string query;

	//! The query executor
	Executor executor;

	Catalog &catalog;
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
	unique_ptr<QueryResult> Query(unique_ptr<SQLStatement> statement, bool allow_stream_result);
	//! Fetch a query from the current result set (if any)
	unique_ptr<DataChunk> Fetch();
	//! Cleanup the result set (if any).
	void Cleanup();
	//! Destroy the client context
	void Destroy();

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
	//! Directly prepare a SQL statement
	unique_ptr<PreparedStatement> Prepare(unique_ptr<SQLStatement> statement);

	//! Execute a prepared statement with the given name and set of parameters
	//! It is possible that the prepared statement will be re-bound. This will generally happen if the catalog is
	//! modified in between the prepared statement being bound and the prepared statement being run.
	unique_ptr<QueryResult> Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
	                                vector<Value> &values, bool allow_stream_result = true);

	//! Register function in the temporary schema
	void RegisterFunction(CreateFunctionInfo *info);

	//! Parse statements from a query
	vector<unique_ptr<SQLStatement>> ParseStatements(string query);

	//! Runs a function with a valid transaction context, potentially starting a transaction if the context is in auto
	//! commit mode.
	void RunFunctionInTransaction(std::function<void(void)> fun, bool requires_valid_transaction = true);
	//! Same as RunFunctionInTransaction, but does not obtain a lock on the client context or check for validation
	void RunFunctionInTransactionInternal(std::function<void(void)> fun, bool requires_valid_transaction = true);

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
	unique_ptr<QueryResult> RunStatementOrPreparedStatement(const string &query, unique_ptr<SQLStatement> statement,
	                                                        shared_ptr<PreparedStatementData> &prepared,
	                                                        vector<Value> *values, bool allow_stream_result);

	//! Internally prepare a SQL statement. Caller must hold the context_lock.
	shared_ptr<PreparedStatementData> CreatePreparedStatement(const string &query, unique_ptr<SQLStatement> statement);
	//! Internally execute a prepared SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> ExecutePreparedStatement(const string &query, shared_ptr<PreparedStatementData> statement,
	                                                 vector<Value> bound_values, bool allow_stream_result);
	//! Call CreatePreparedStatement() and ExecutePreparedStatement() without any bound values
	unique_ptr<QueryResult> RunStatementInternal(const string &query, unique_ptr<SQLStatement> statement,
	                                             bool allow_stream_result);
	unique_ptr<PreparedStatement> PrepareInternal(unique_ptr<SQLStatement> statement);
	void LogQueryInternal(string query);

private:
	//! The currently opened StreamQueryResult (if any)
	StreamQueryResult *open_result = nullptr;
};

} // namespace duckdb
