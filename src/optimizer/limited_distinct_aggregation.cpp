#include "duckdb/optimizer/limited_distinct_aggregation.hpp"

#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

void LimitedDistinctAggregation::PushdownLimitIntoDistinct(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_LIMIT) {
		return;
	}
	auto &limit = op.Cast<LogicalLimit>();
	if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE || limit.offset_val.Type() != LimitNodeType::CONSTANT_VALUE) {
		return;
	}

	auto *child = op.children[0].get();
	if (child->type != LogicalOperatorType::LOGICAL_DISTINCT) {
		return;
	}
	auto &distinct = child->Cast<LogicalDistinct>();
	if (distinct.distinct_type != DistinctType::DISTINCT) {
		return;
	}

	idx_t limit_value = limit.limit_val.GetConstantValue();
	idx_t offset_value = limit.offset_val.GetConstantValue();
	distinct.limit = limit_value + offset_value;
}

unique_ptr<LogicalOperator> LimitedDistinctAggregation::Optimize(unique_ptr<LogicalOperator> op) {
	PushdownLimitIntoDistinct(*op);
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	return op;
}

} // namespace duckdb
