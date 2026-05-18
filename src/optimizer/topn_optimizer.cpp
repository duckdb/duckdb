#include "duckdb/optimizer/topn_optimizer.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

namespace duckdb {

namespace {

bool TopNDynamicFilterTypeSupported(const LogicalType &type) {
	return TypeIsNumeric(type.InternalType()) || type.id() == LogicalTypeId::VARCHAR;
}

ExpressionType TopNComparisonType(const OrderType order_type, const bool single_order) {
	if (order_type == OrderType::ASCENDING) {
		return single_order ? ExpressionType::COMPARE_LESSTHAN : ExpressionType::COMPARE_LESSTHANOREQUALTO;
	}
	return single_order ? ExpressionType::COMPARE_GREATERTHAN : ExpressionType::COMPARE_GREATERTHANOREQUALTO;
}

bool TryGetGlobalOrderBoundary(ClientContext &context, LogicalGet &get, const ColumnBinding &binding,
                               const BoundOrderByNode &order, Value &boundary, idx_t &boundary_groups) {
	if (!get.function.get_partition_stats || order.null_order != OrderByNullType::NULLS_LAST) {
		return false;
	}
	const auto &type = order.expression->GetReturnType();
	if (type.id() == LogicalTypeId::VARCHAR || !TypeIsNumeric(type.InternalType())) {
		return false;
	}

	const auto &column_index = get.GetColumnIndex(binding);
	StorageIndex storage_index;
	if (!get.TryGetStorageIndex(column_index, storage_index)) {
		return false;
	}

	GetPartitionStatsInput input(get.function, get.bind_data.get());
	auto partition_stats = get.function.get_partition_stats(context, input);
	if (partition_stats.empty()) {
		return false;
	}

	const auto order_by = order.type == OrderType::ASCENDING ? OrderByStatistics::MIN : OrderByStatistics::MAX;
	bool has_boundary = false;
	for (auto &partition_stat : partition_stats) {
		if (partition_stat.count_type == CountType::COUNT_APPROXIMATE || !partition_stat.partition_row_group) {
			return false;
		}
		auto stats = partition_stat.partition_row_group->GetColumnStatistics(storage_index);
		auto value = RowGroupReorderer::RetrieveStat(*stats, order_by, OrderByColumnType::NUMERIC);
		if (value.IsNull()) {
			if (order.null_order == OrderByNullType::NULLS_LAST && !stats->CanHaveNoNull()) {
				continue;
			}
			return false;
		}
		if (!has_boundary || (order.type == OrderType::ASCENDING && ValueOperations::LessThan(value, boundary)) ||
		    (order.type == OrderType::DESCENDING && ValueOperations::GreaterThan(value, boundary))) {
			boundary = std::move(value);
			has_boundary = true;
		}
	}
	if (!has_boundary) {
		return false;
	}

	for (auto &partition_stat : partition_stats) {
		auto stats = partition_stat.partition_row_group->GetColumnStatistics(storage_index);
		auto value = RowGroupReorderer::RetrieveStat(*stats, order_by, OrderByColumnType::NUMERIC);
		if (!value.IsNull() && ValueOperations::NotDistinctFrom(value, boundary)) {
			boundary_groups++;
		}
	}
	return true;
}

} // namespace

TopN::TopN(ClientContext &context_p) : context(context_p) {
}

bool TopN::CanOptimize(LogicalOperator &op, optional_ptr<ClientContext> context) {
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
		if (context) {
			// estimate child cardinality if the context is available
			child_op->EstimateCardinality(*context);
		}

		if (child_op->has_estimated_cardinality) {
			// only check if we should switch to full sorting if we have estimated cardinality
			auto constant_limit = static_cast<double>(limit.limit_val.GetConstantValue());
			if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				constant_limit += static_cast<double>(limit.offset_val.GetConstantValue());
			}
			auto child_card = static_cast<double>(child_op->estimated_cardinality);

			// if the limit is > 0.7% of the child cardinality, sorting the whole table is faster
			bool limit_is_large = constant_limit > 5000;
			if (constant_limit > child_card * 0.007 && limit_is_large) {
				return false;
			}
		}

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
	bool nulls_first = op.orders[0].null_order == OrderByNullType::NULLS_FIRST;
	auto &type = op.orders[0].expression->GetReturnType();
	if (!TopNDynamicFilterTypeSupported(type)) {
		// only supported for numeric and varchar types
		return;
	}
	if (op.orders[0].expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		// we can only pushdown on ORDER BY [col] currently
		return;
	}
	if (op.dynamic_filter) {
		// dynamic filter is already set
		return;
	}
	auto &colref = op.orders[0].expression->Cast<BoundColumnRefExpression>();
	vector<JoinFilterPushdownColumn> columns;
	JoinFilterPushdownColumn column;
	column.probe_column_index = colref.binding;
	columns.emplace_back(column);
	vector<PushdownFilterTarget> pushdown_targets;
	JoinFilterPushdownOptimizer::GetPushdownFilterTargets(*op.children[0], std::move(columns), pushdown_targets);
	if (pushdown_targets.empty()) {
		// no pushdown targets
		return;
	}
	// found pushdown targets! generate dynamic filters
	ExpressionType comparison_type = TopNComparisonType(op.orders[0].type, op.orders.size() == 1);
	Value minimum_value = type.InternalType() == PhysicalType::VARCHAR ? Value("") : Value::MinimumValue(type);
	auto filter_data = make_shared_ptr<DynamicFilterData>(comparison_type, std::move(minimum_value));

	// put the filter into the Top-N clause
	op.dynamic_filter = filter_data;

	for (auto &target : pushdown_targets) {
		auto &get = target.get;
		D_ASSERT(target.columns.size() == 1);
		auto col_binding = target.columns[0].probe_column_index;

		// create the actual dynamic filter
		auto pushed_expr = CreateDynamicFilterExpression(filter_data, type);
		if (nulls_first) {
			auto or_filter = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
			auto is_null = ExpressionFilter::CreateNullCheckExpression(
			    make_uniq<BoundReferenceExpression>(type, idx_t(0)), ExpressionType::OPERATOR_IS_NULL);
			or_filter->children.push_back(std::move(is_null));
			or_filter->children.push_back(std::move(pushed_expr));
			pushed_expr = std::move(or_filter);
		}

		// push the filter into the table scan
		get.table_filters.PushFilter(
		    col_binding.column_index,
		    make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(std::move(pushed_expr), type)));
	}

	if (op.orders.size() < 2 || op.orders[0].null_order != OrderByNullType::NULLS_LAST ||
	    op.orders[1].null_order != OrderByNullType::NULLS_LAST ||
	    op.orders[1].expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return;
	}
	auto &second_type = op.orders[1].expression->GetReturnType();
	if (!TypeIsNumeric(type.InternalType()) || !TypeIsNumeric(second_type.InternalType())) {
		return;
	}

	auto &second_colref = op.orders[1].expression->Cast<BoundColumnRefExpression>();
	vector<JoinFilterPushdownColumn> multi_columns;
	JoinFilterPushdownColumn first_column;
	first_column.probe_column_index = colref.binding;
	multi_columns.push_back(std::move(first_column));
	JoinFilterPushdownColumn second_column;
	second_column.probe_column_index = second_colref.binding;
	multi_columns.push_back(std::move(second_column));

	vector<PushdownFilterTarget> multi_targets;
	JoinFilterPushdownOptimizer::GetPushdownFilterTargets(*op.children[0], std::move(multi_columns), multi_targets);
	if (multi_targets.size() != 1 || multi_targets[0].columns.size() != 2) {
		return;
	}

	auto &target = multi_targets[0];
	Value prefix_boundary;
	idx_t prefix_boundary_groups = 0;
	if (!TryGetGlobalOrderBoundary(context, target.get, target.columns[0].probe_column_index, op.orders[0],
	                               prefix_boundary, prefix_boundary_groups) ||
	    prefix_boundary_groups <= 1) {
		return;
	}

	auto second_filter_data = make_shared_ptr<DynamicFilterData>(TopNComparisonType(op.orders[1].type, false),
	                                                             Value::MinimumValue(second_type));
	op.secondary_dynamic_filter = second_filter_data;
	op.secondary_dynamic_filter_prefix = std::move(prefix_boundary);

	auto pushed_expr = CreateDynamicFilterExpression(std::move(second_filter_data), second_type);
	auto col_binding = target.columns[1].probe_column_index;
	target.get.table_filters.PushFilter(
	    col_binding.column_index,
	    make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(std::move(pushed_expr), second_type)));
}

unique_ptr<LogicalOperator> TopN::Optimize(unique_ptr<LogicalOperator> op) {
	if (CanOptimize(*op, &context)) {
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
	if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
		PushdownDynamicFilters(op->Cast<LogicalTopN>());
	}

	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	return op;
}

} // namespace duckdb
