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
	TransactionData(transaction_t transaction_id_p, transaction_t start_time_p);

	optional_ptr<DuckTransaction> transaction;
	transaction_t transaction_id;
	transaction_t start_time;

	static TransactionData Committed() {
		return TransactionData(MAX_TRANSACTION_ID, 0);
	}
};

//! A snapshot as of start_time sees exactly the commits strictly before it. A durability-bounded
//! snapshot can start AT a commit id: that commit is not part of the snapshot.
inline bool VisibleToSnapshot(transaction_t timestamp, transaction_t start_time) {
	return timestamp < start_time;
}

} // namespace duckdb
