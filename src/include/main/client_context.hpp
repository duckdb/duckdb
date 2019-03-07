//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/client_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_set.hpp"
#include "common/allocator.hpp"
#include "execution/execution_context.hpp"
#include "main/query_profiler.hpp"
#include "transaction/transaction_context.hpp"

namespace duckdb {
class DuckDB;

//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext {
public:
	ClientContext(DuckDB &database);

	Transaction &ActiveTransaction() {
		return transaction.ActiveTransaction();
	}

	void Interrupt() {
		interrupted = true;
	}

	unique_ptr<DuckDBStreamingResult> Query(string query);
	unique_ptr<DataChunk> Fetch(int64_t query_number);
	bool Cleanup(int64_t query_number = -1);

	//! The allocator that holds any allocations made in the Query Context
	Allocator allocator;
	//! Query profiler
	QueryProfiler profiler;
	//! The database that this client is connected to
	DuckDB &db;
	//! Data for the currently running transaction
	TransactionContext transaction;
	//! Whether or not the query is interrupted
	bool interrupted;

	ExecutionContext execution_context;

	//	unique_ptr<CatalogSet> temporary_tables;
	unique_ptr<CatalogSet> prepared_statements;

#ifdef DEBUG
	// Whether or not aggressive query verification is enabled
	bool query_verification_enabled = false;
	//! Enable the running of optimizers
	bool enable_optimizer = true;
#endif
};
} // namespace duckdb
