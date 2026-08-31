#include "duckdb/planner/operator/logical_load.hpp"

namespace duckdb {

LogicalLoad::LogicalLoad(unique_ptr<LoadInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_LOAD), info(std::move(info_p)) {
}

LogicalLoad::LogicalLoad(unique_ptr<ParseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_LOAD),
      info(unique_ptr_cast<ParseInfo, LoadInfo>(std::move(info_p))) {
}

LogicalLoad::~LogicalLoad() {
}

idx_t LogicalLoad::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalLoad::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace duckdb
