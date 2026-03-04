#include "duckdb/optimizer/limited_distinct_aggregation.hpp"

#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

bool LimitedDistinctAggregation::CanOptimize(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_LIMIT) {
		return false;
	}
	auto &limit = op.Cast<LogicalLimit>();
	if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
		return false;
	}
	if (limit.offset_val.Type() != LimitNodeType::CONSTANT_VALUE && limit.offset_val.Type() != LimitNodeType::UNSET) {
		return false;
	}

	auto *child = op.children[0].get();
	if (child->type != LogicalOperatorType::LOGICAL_DISTINCT) {
		return false;
	}
	auto &distinct = child->Cast<LogicalDistinct>();
	if (distinct.distinct_type != DistinctType::DISTINCT) {
		return false;
	}
	return true;
}

unique_ptr<LogicalOperator> LimitedDistinctAggregation::Optimize(unique_ptr<LogicalOperator> op) {
	if (CanOptimize(*op)) {
		auto &limit = op->Cast<LogicalLimit>();
		auto *child = op->children[0].get();
		auto &distinct = child->Cast<LogicalDistinct>();

		idx_t limit_value = limit.limit_val.GetConstantValue();
		idx_t offset_value = 0;
		if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			offset_value = limit.offset_val.GetConstantValue();
		}
		distinct.limit = limit_value + offset_value;
	}
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	return op;
}

} // namespace duckdb
