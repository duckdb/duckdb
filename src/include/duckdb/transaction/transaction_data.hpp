//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class DuckTransaction;
class Transaction;

struct TransactionData {
	TransactionData(DuckTransaction &transaction_p);
	TransactionData(transaction_t transaction_id_p, transaction_t start_time_p);

	DuckTransaction *transaction;
	transaction_t transaction_id;
	transaction_t start_time;
};

} // namespace duckdb
