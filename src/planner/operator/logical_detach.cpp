#include "duckdb/planner/operator/logical_detach.hpp"

namespace duckdb {

LogicalDetach::LogicalDetach(unique_ptr<DetachInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DETACH), info(std::move(info_p)) {
}

LogicalDetach::LogicalDetach(unique_ptr<ParseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DETACH),
      info(unique_ptr_cast<ParseInfo, DetachInfo>(std::move(info_p))) {
}

LogicalDetach::~LogicalDetach() {
}

idx_t LogicalDetach::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalDetach::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace duckdb
