#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

LogicalTopN::LogicalTopN(vector<BoundOrderByNode> orders, idx_t limit, idx_t offset)
    : LogicalOperator(LogicalOperatorType::LOGICAL_TOP_N), orders(std::move(orders)), limit(limit), offset(offset) {
}

LogicalTopN::~LogicalTopN() {
}

idx_t LogicalTopN::EstimateCardinality(ClientContext &context) {
	auto child_cardinality = LogicalOperator::EstimateCardinality(context);
	if (child_cardinality < limit) {
		return child_cardinality;
	}
	return limit;
}

} // namespace duckdb
