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
class Transaction;

struct TransactionData {
	TransactionData(Transaction &transaction_p);
	TransactionData(transaction_t transaction_id_p, transaction_t start_time_p);

	Transaction *transaction;
	transaction_t transaction_id;
	transaction_t start_time;
};

} // namespace duckdb
