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
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/error_data.hpp"

namespace duckdb {
class AttachedDatabase;
class ClientContext;
class Transaction;

//! The MetaTransaction manages multiple transactions for different attached databases
class MetaTransaction {
public:
	DUCKDB_API MetaTransaction(ClientContext &context, timestamp_t start_timestamp);

	ClientContext &context;
	//! The timestamp when the transaction started
	timestamp_t start_timestamp;
	//! The validity checker of the transaction
	ValidChecker transaction_validity;
	//! The active query number
	transaction_t active_query;

public:
	DUCKDB_API static MetaTransaction &Get(ClientContext &context);
	timestamp_t GetCurrentTransactionStartTimestamp() const {
		return start_timestamp;
	}

	Transaction &GetTransaction(AttachedDatabase &db);
	optional_ptr<Transaction> TryGetTransaction(AttachedDatabase &db);
	void RemoveTransaction(AttachedDatabase &db);

	ErrorData Commit();
	void Rollback();

	idx_t GetActiveQuery();
	void SetActiveQuery(transaction_t query_number);

	void SetReadOnly();
	bool IsReadOnly() const;
	void ModifyDatabase(AttachedDatabase &db);
	optional_ptr<AttachedDatabase> ModifiedDatabase() {
		return modified_database;
	}
	const vector<reference<AttachedDatabase>> &OpenedTransactions() const {
		return all_transactions;
	}

private:
	//! Lock to prevent all_transactions and transactions from getting out of sync
	mutex lock;
	//! The set of active transactions for each database
	reference_map_t<AttachedDatabase, reference<Transaction>> transactions;
	//! The set of transactions in order of when they were started
	vector<reference<AttachedDatabase>> all_transactions;
	//! The database we are modifying - we can only modify one database per transaction
	optional_ptr<AttachedDatabase> modified_database;
	//! Whether or not the meta transaction is marked as read only
	bool is_read_only;
};

} // namespace duckdb
