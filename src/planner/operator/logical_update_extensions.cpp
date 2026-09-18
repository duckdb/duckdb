#include "duckdb/planner/operator/logical_update_extensions.hpp"

namespace duckdb {

LogicalUpdateExtensions::LogicalUpdateExtensions(unique_ptr<UpdateExtensionsInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE_EXTENSIONS), info(std::move(info_p)) {
}

LogicalUpdateExtensions::~LogicalUpdateExtensions() {
}

idx_t LogicalUpdateExtensions::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalUpdateExtensions::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace duckdb
