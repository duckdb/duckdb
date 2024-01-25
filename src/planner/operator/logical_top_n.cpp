#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

idx_t LogicalTopN::EstimateCardinality(ClientContext &context) {
	auto child_cardinality = LogicalOperator::EstimateCardinality(context);
	if (limit >= 0 && child_cardinality < idx_t(limit)) {
		return limit;
	}
	return child_cardinality;
}

} // namespace duckdb
