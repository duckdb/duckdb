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

vector<ColumnBinding> LogicalLoad::GetColumnBindings() {
	return GenerateColumnBindings(TableIndex(0), LoadInfo::GetResultTypes(info->load_type).size());
}

void LogicalLoad::ResolveTypes() {
	types = LoadInfo::GetResultTypes(info->load_type);
}

} // namespace duckdb
