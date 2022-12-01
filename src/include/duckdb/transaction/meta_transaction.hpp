//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/meta_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class AttachedDatabase;
class ClientContext;
class Transaction;

//! The MetaTransaction manages multiple transactions for different attached databases
class MetaTransaction {
public:
	MetaTransaction(ClientContext &context, timestamp_t start_timestamp, idx_t catalog_version);

	ClientContext &context;
	//! The timestamp when the transaction started
	timestamp_t start_timestamp;
	//! The catalog version when the transaction was started
	idx_t catalog_version;
	//! The validity checker of the transaction
	ValidChecker transaction_validity;
	//! The set of active transactions for each database
	unordered_map<AttachedDatabase *, Transaction *> transactions;
	//! Whether or not any transaction have made modifications
	bool read_only;
	//! The active query number
	transaction_t active_query;

public:
	static MetaTransaction &Get(ClientContext &context);
	timestamp_t GetCurrentTransactionStartTimestamp() {
		return start_timestamp;
	}

	string Commit();
	void Rollback();

	idx_t GetActiveQuery();
	void SetActiveQuery(transaction_t query_number);
};

} // namespace duckdb
