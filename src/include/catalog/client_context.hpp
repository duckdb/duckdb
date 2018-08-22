//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/client_context.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.hpp"
#include "storage/transaction_context.hpp"

namespace duckdb {

//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext {
  public:
	ClientContext(Catalog &catalog, TransactionContext &transaction)
	    : catalog(catalog), transaction(transaction) {}

	//! The catalog that this client is connected to
	Catalog &catalog;
	//! Data for the currently running transaction
	TransactionContext &transaction;
};
} // namespace duckdb
