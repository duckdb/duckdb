#include "duckdb/planner/operator/logical_alter.hpp"

namespace duckdb {

LogicalAlter::LogicalAlter(unique_ptr<AlterInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_ALTER), info(std::move(info_p)) {
}

LogicalAlter::LogicalAlter(unique_ptr<ParseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_ALTER),
      info(unique_ptr_cast<ParseInfo, AlterInfo>(std::move(info_p))) {
}

LogicalAlter::~LogicalAlter() {
}

idx_t LogicalAlter::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalAlter::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace duckdb
