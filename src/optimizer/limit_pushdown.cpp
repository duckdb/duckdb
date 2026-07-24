#include "duckdb/optimizer/limit_pushdown.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

optional_idx GetLimitPushdownValue(const LogicalLimit &limit) {
	if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
		return optional_idx();
	}
	idx_t offset_value = 0;
	switch (limit.offset_val.Type()) {
	case LimitNodeType::UNSET:
		break;
	case LimitNodeType::CONSTANT_VALUE:
		offset_value = limit.offset_val.GetConstantValue();
		break;
	default:
		return optional_idx();
	}

	auto limit_value = limit.limit_val.GetConstantValue();
	if (offset_value > NumericLimits<idx_t>::Maximum() - limit_value) {
		return optional_idx();
	}
	return limit_value + offset_value;
}

bool CanPushdownIntoGet(LogicalGet &get) {
	return get.children.empty() && get.input_table_types.empty() && get.input_table_names.empty() &&
	       get.projected_input.empty() && !get.table_filters.HasFilters();
}

void PushdownLimitIntoScan(LogicalOperator &op, optional_idx limit_value) {
	if (!limit_value.IsValid()) {
		return;
	}

	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if (op.children.size() != 1) {
			return;
		}
		PushdownLimitIntoScan(*op.children[0], limit_value);
		return;
	}

	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &get = op.Cast<LogicalGet>();
	if (!CanPushdownIntoGet(get)) {
		return;
	}
	if (!get.limit.IsValid() || limit_value.GetIndex() < get.limit.GetIndex()) {
		get.limit = limit_value;
	}
}

void PushdownLimitIntoScan(LogicalLimit &limit) {
	if (limit.children.empty()) {
		return;
	}
	PushdownLimitIntoScan(*limit.children[0], GetLimitPushdownValue(limit));
}

} // namespace

bool LimitPushdown::CanOptimize(duckdb::LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_LIMIT &&
	    op.children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &limit = op.Cast<LogicalLimit>();

		if (limit.offset_val.Type() == LimitNodeType::EXPRESSION_PERCENTAGE ||
		    limit.offset_val.Type() == LimitNodeType::EXPRESSION_VALUE) {
			// Offset cannot be an expression
			return false;
		}

		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE && limit.limit_val.GetConstantValue() < 8192) {
			// Push down only when limit value is smaller than 8192.
			// when physical_limit is introduced, it will end a parallel pipeline
			// restrict the limit value to be small so that remaining operations run fast without parallelization.
			return true;
		}
	}
	return false;
}

unique_ptr<LogicalOperator> LimitPushdown::Optimize(unique_ptr<LogicalOperator> op) {
	if (CanOptimize(*op)) {
		auto projection = std::move(op->children[0]);
		op->children[0] = std::move(projection->children[0]);
		projection->SetEstimatedCardinality(op->estimated_cardinality);
		projection->children[0] = std::move(op);
		swap(projection, op);
	}
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		PushdownLimitIntoScan(op->Cast<LogicalLimit>());
	}
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	return op;
}

} // namespace duckdb
