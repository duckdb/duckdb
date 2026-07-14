#include "duckdb/planner/operator/logical_attach.hpp"

namespace duckdb {

LogicalAttach::LogicalAttach(unique_ptr<AttachInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_ATTACH), info(std::move(info_p)) {
}

LogicalAttach::LogicalAttach(unique_ptr<ParseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_ATTACH),
      info(unique_ptr_cast<ParseInfo, AttachInfo>(std::move(info_p))) {
}

LogicalAttach::~LogicalAttach() {
}

idx_t LogicalAttach::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalAttach::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace duckdb
