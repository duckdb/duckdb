#include "duckdb/optimizer/row_group_pruner.hpp"

#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

RowGroupPruner::RowGroupPruner(ClientContext &context_p) : context(context_p) {
}

unique_ptr<LogicalOperator> RowGroupPruner::Optimize(unique_ptr<LogicalOperator> op) {
	auto parameters = CanOptimize(*op, context);
	if (!parameters) {
		for (auto &child : op->children) {
			child = Optimize(std::move(child));
		}
		return op;
	}

	parameters->logical_get.get().function.set_scan_order(std::move(parameters->options),
	                                                      parameters->logical_get.get().bind_data.get());

	return op;
}

unique_ptr<RowGroupPrunerParameters> RowGroupPruner::CanOptimize(LogicalOperator &op,
                                                                 optional_ptr<ClientContext> context) {
	optional_idx row_limit;
	optional_idx row_offset;
	if (op.type != LogicalOperatorType::LOGICAL_LIMIT) {
		return nullptr;
	}

	auto &logical_limit = op.Cast<LogicalLimit>();
	if (logical_limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		row_limit = logical_limit.limit_val.GetConstantValue();
		if (logical_limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			row_offset = logical_limit.offset_val.GetConstantValue();
		} else if (logical_limit.offset_val.Type() == LimitNodeType::UNSET) {
			row_offset = 0;
		}
	}

	reference<LogicalOperator> current_op = *op.children[0];
	while (current_op.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		current_op = *current_op.get().children[0];
	}

	if (current_op.get().type != LogicalOperatorType::LOGICAL_ORDER_BY) {
		return nullptr;
	}

	auto &logical_order = current_op.get().Cast<LogicalOrder>();
	for (const auto &order : logical_order.orders) {
		// We do not support any null-first orders as this requires unimplemented logic in the row group reorderer
		if (order.null_order == OrderByNullType::NULLS_FIRST) {
			row_limit.SetInvalid();
			row_offset.SetInvalid();
			break;
		}
	}

	auto order_column_type = logical_order.orders[0].expression->return_type;
	if (!order_column_type.IsNumeric() && !order_column_type.IsTemporal() &&
	    order_column_type != LogicalType::VARCHAR) {
		return nullptr;
	}

	// Only reorder row groups if there are no additional limit operators since they could modify the order
	while (!current_op.get().children.empty()) {
		if (current_op.get().children.size() > 1) {
			return nullptr;
		}

		const auto op_type = current_op.get().type;
		if (op_type == LogicalOperatorType::LOGICAL_LIMIT) {
			return nullptr;
		}
		if (op_type == LogicalOperatorType::LOGICAL_FILTER ||
		    op_type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			// TODO: use a row limit if this is a simple min/max aggregation without groups
			row_limit.SetInvalid();
			row_offset.SetInvalid();
		}
		current_op = *current_op.get().children[0];
	}

	auto &colref = logical_order.orders[0].expression->Cast<BoundColumnRefExpression>();

	vector<JoinFilterPushdownColumn> columns {JoinFilterPushdownColumn {colref.binding}};
	vector<PushdownFilterTarget> pushdown_targets;
	JoinFilterPushdownOptimizer::GetPushdownFilterTargets(*op.children[0], std::move(columns), pushdown_targets);
	if (pushdown_targets.empty()) {
		return nullptr;
	}
	D_ASSERT(pushdown_targets.size() == 1);
	auto &logical_get = pushdown_targets.front().get;

	if (!logical_get.table_filters.filters.empty()) {
		row_limit.SetInvalid();
		row_offset.SetInvalid();
	}

	if (logical_get.function.set_scan_order) {
		auto column_type =
		    colref.return_type == LogicalType::VARCHAR ? OrderByColumnType::STRING : OrderByColumnType::NUMERIC;
		auto order_type =
		    logical_order.orders[0].type == OrderType::ASCENDING ? RowGroupOrderType::ASC : RowGroupOrderType::DESC;
		auto order_by = order_type == RowGroupOrderType::ASC ? OrderByStatistics::MIN : OrderByStatistics::MAX;
		auto col_idx = pushdown_targets[0].columns[0].probe_column_index.column_index;
		auto &column_index = logical_get.GetColumnIds()[col_idx];
		auto options = make_uniq<RowGroupOrderOptions>(column_index.GetPrimaryIndex(), order_by, order_type,
		                                               column_type, row_limit, row_offset);

		if (logical_get.function.get_partition_stats && row_offset.IsValid() && row_offset.GetIndex() > 0) {
			GetPartitionStatsInput input(logical_get.function, logical_get.bind_data.get());
			auto partition_stats = logical_get.function.get_partition_stats(*context, input);

			if (!partition_stats.empty()) {
				auto new_offset = RowGroupReorderer::GetOffsetAfterPruning(*options, partition_stats);
				// TODO: set this somewhere else. Also, set a value in options so that we know to prune the first N
				// rowgroups
				logical_limit.offset_val = BoundLimitNode::ConstantValue(static_cast<int64_t>(new_offset));
			}
		}

		return make_uniq<RowGroupPrunerParameters>(std::move(options), logical_get);
	}

	return nullptr;
}

} // namespace duckdb
