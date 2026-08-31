//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class DuckTransaction;
class Transaction;

struct TransactionData {
	TransactionData(DuckTransaction &transaction_p); // NOLINT: allow implicit conversion
	TransactionData(transaction_t transaction_id_p, transaction_t snapshot_bound_p);

	optional_ptr<DuckTransaction> transaction;
	transaction_t transaction_id;
	transaction_t snapshot_bound;

	static TransactionData Committed() {
		return TransactionData(MAX_TRANSACTION_ID, 0);
	}
};

//! Commit ids fall below TRANSACTION_ID_START and transaction ids above it, so a stamp says by
//! itself whether the transaction that wrote it committed. Catalog versions use the same split.
inline bool IsCommitted(transaction_t timestamp) {
	return timestamp < TRANSACTION_ID_START;
}

//! All stamps < snapshot_bound are visible to the snapshot; all stamps >= it are invisible. Not
//! every bound is a transaction's start time: callers also derive one from a commit id, or from
//! the last commit.
inline bool VisibleToSnapshot(transaction_t timestamp, transaction_t snapshot_bound) {
	return timestamp < snapshot_bound;
}

} // namespace duckdb
