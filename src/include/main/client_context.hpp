//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// main/client_context.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "main/database.hpp"
#include "main/query_profiler.hpp"

#include "transaction/transaction_context.hpp"

namespace duckdb {
//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext {
  public:
	ClientContext(DuckDB &database)
	    : db(database), transaction(database.transaction_manager) {}

	Transaction &ActiveTransaction() { return transaction.ActiveTransaction(); }

	//! Query profiler
	QueryProfiler profiler;
	//! The database that this client is connected to
	DuckDB &db;
	//! Data for the currently running transaction
	TransactionContext transaction;
};
} // namespace duckdb
