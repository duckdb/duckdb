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
class DTransaction;
class Transaction;

struct TransactionData {
	TransactionData(DTransaction &transaction_p);
	TransactionData(transaction_t transaction_id_p, transaction_t start_time_p);

	DTransaction *transaction;
	transaction_t transaction_id;
	transaction_t start_time;
};

} // namespace duckdb
