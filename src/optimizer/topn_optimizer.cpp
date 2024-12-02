#include "duckdb/optimizer/topn_optimizer.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"

namespace duckdb {

bool TopN::CanOptimize(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op.Cast<LogicalLimit>();

		if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
			// we need LIMIT to be present AND be a constant value for us to be able to use Top-N
			return false;
		}
		if (limit.offset_val.Type() == LimitNodeType::EXPRESSION_VALUE) {
			// we need offset to be either not set (i.e. limit without offset) OR have offset be
			return false;
		}

		auto child_op = op.children[0].get();

		while (child_op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			D_ASSERT(!child_op->children.empty());
			child_op = child_op->children[0].get();
		}

		return child_op->type == LogicalOperatorType::LOGICAL_ORDER_BY;
	}
	return false;
}

void TopN::PushdownDynamicFilters(LogicalTopN &op) {
	// pushdown dynamic filters through the Top-N operator
	if (op.orders[0].null_order == OrderByNullType::NULLS_FIRST) {
		// FIXME: not supported for NULLS FIRST quite yet
		return;
	}
	auto &type = op.orders[0].expression->return_type;
	if (!TypeIsIntegral(type.InternalType())) {
		// only supported for integral types currently
		return;
	}
	if (op.orders[0].expression->type != ExpressionType::BOUND_COLUMN_REF) {
		// we can only pushdown on ORDER BY [col] currently
		return;
	}
	auto &colref = op.orders[0].expression->Cast<BoundColumnRefExpression>();
	vector<JoinFilterPushdownColumn> columns;
	JoinFilterPushdownColumn column;
	column.probe_column_index = colref.binding;
	columns.emplace_back(std::move(column));
	vector<PushdownFilterTarget> pushdown_targets;
	JoinFilterPushdownOptimizer::GetPushdownFilterTargets(*op.children[0], std::move(columns), pushdown_targets);
	if (pushdown_targets.empty()) {
		// no pushdown targets
		return;
	}
	// found pushdown targets! generate dynamic filters
	unique_ptr<TableFilter> base_filter;
	if (op.orders[0].type == OrderType::ASCENDING) {
		// for ascending order, we want the lowest N elements, so we filter on C <= [boundary]
		// note: if we only have a single order clause, we could filter on C < boundary instead
		// but that doesn't work with us pushing the sentinel value here
		auto initial_bound = Value::MaximumValue(type);
		base_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(initial_bound));
	} else {
		// for descending order, we want the highest N elements, so we filter on C >= [boundary]
		auto initial_bound = Value::MinimumValue(type);
		base_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(initial_bound));
	}
	auto filter_data = make_shared_ptr<DynamicFilterData>();
	filter_data->filter = std::move(base_filter);

	// put the filter into the Top-N clause
	op.dynamic_filter = filter_data;

	for (auto &target : pushdown_targets) {
		auto &get = target.get;
		D_ASSERT(target.columns.size() == 1);
		auto col_idx = target.columns[0].probe_column_index.column_index;

		// create the actual dynamic filter
		auto dynamic_filter = make_uniq<DynamicFilter>(filter_data);
		auto optional_filter = make_uniq<OptionalFilter>(std::move(dynamic_filter));

		// push the filter into the table scan
		auto &column_index = get.GetColumnIds()[col_idx];
		get.table_filters.PushFilter(column_index, std::move(optional_filter));
	}
}

unique_ptr<LogicalOperator> TopN::Optimize(unique_ptr<LogicalOperator> op) {
	if (CanOptimize(*op)) {

		vector<unique_ptr<LogicalOperator>> projections;

		// traverse operator tree and collect all projection nodes until we reach
		// the order by operator

		auto child = std::move(op->children[0]);
		// collect all projections until we get to the order by
		while (child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			D_ASSERT(!child->children.empty());
			auto tmp = std::move(child->children[0]);
			projections.push_back(std::move(child));
			child = std::move(tmp);
		}
		D_ASSERT(child->type == LogicalOperatorType::LOGICAL_ORDER_BY);
		auto &order_by = child->Cast<LogicalOrder>();

		// Move order by operator into children of limit operator
		op->children[0] = std::move(child);

		auto &limit = op->Cast<LogicalLimit>();
		auto limit_val = limit.limit_val.GetConstantValue();
		idx_t offset_val = 0;
		if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			offset_val = limit.offset_val.GetConstantValue();
		}
		auto topn = make_uniq<LogicalTopN>(std::move(order_by.orders), limit_val, offset_val);
		topn->AddChild(std::move(order_by.children[0]));
		auto cardinality = limit_val;
		if (topn->children[0]->has_estimated_cardinality && topn->children[0]->estimated_cardinality < limit_val) {
			cardinality = topn->children[0]->estimated_cardinality;
		}
		PushdownDynamicFilters(*topn);
		topn->SetEstimatedCardinality(cardinality);
		op = std::move(topn);

		// reconstruct all projection nodes above limit operator
		while (!projections.empty()) {
			auto node = std::move(projections.back());
			node->children[0] = std::move(op);
			op = std::move(node);
			projections.pop_back();
		}
	}

	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	return op;
}

} // namespace duckdb
