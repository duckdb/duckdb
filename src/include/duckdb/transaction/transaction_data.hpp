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
};

} // namespace duckdb
