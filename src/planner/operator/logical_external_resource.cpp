#include "duckdb/planner/operator/logical_external_resource.hpp"

namespace duckdb {

LogicalExternalResource::LogicalExternalResource(BoundExternalResource data_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EXTERNAL_RESOURCE), data(std::move(data_p)) {
}

idx_t LogicalExternalResource::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalExternalResource::ResolveTypes() {
	if (data.operation == ExternalResourceOperation::SHOW) {
		// SHOW is handled as a table-function scan, not this operator.
		throw InternalException("LogicalExternalResource does not handle SHOW");
	}
	types = {LogicalType::BOOLEAN};
}

} // namespace duckdb
