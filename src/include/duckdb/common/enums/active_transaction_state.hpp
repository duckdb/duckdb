//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/active_transaction_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

enum class ActiveTransactionState { UNSET, OTHER_TRANSACTIONS, NO_OTHER_TRANSACTIONS };

} // namespace duckdb
