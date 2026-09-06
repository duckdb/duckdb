#include "duckdb/planner/operator/logical_drop.hpp"

namespace duckdb {

LogicalDrop::LogicalDrop(unique_ptr<DropInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DROP), info(std::move(info_p)) {
}

LogicalDrop::LogicalDrop(unique_ptr<ParseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DROP),
      info(unique_ptr_cast<ParseInfo, DropInfo>(std::move(info_p))) {
}

LogicalDrop::~LogicalDrop() {
}

idx_t LogicalDrop::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalDrop::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace duckdb
