#include "duckdb/planner/operator/logical_transaction.hpp"

namespace duckdb {

LogicalTransaction::LogicalTransaction(unique_ptr<TransactionInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_TRANSACTION), info(std::move(info_p)) {
}

LogicalTransaction::LogicalTransaction(unique_ptr<ParseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_TRANSACTION),
      info(unique_ptr_cast<ParseInfo, TransactionInfo>(std::move(info_p))) {
}

LogicalTransaction::~LogicalTransaction() {
}

idx_t LogicalTransaction::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalTransaction::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace duckdb
