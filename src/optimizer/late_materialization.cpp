#include "duckdb/optimizer/late_materialization.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

LateMaterialization::LateMaterialization(Optimizer &optimizer) : optimizer(optimizer) {
}

idx_t LateMaterialization::GetOrInsertRowId(LogicalGet &get) {
	auto &column_ids = get.GetMutableColumnIds();
	// check if it is already projected
	for (idx_t i = 0; i < column_ids.size(); ++i) {
		if (column_ids[i].IsRowIdColumn()) {
			// already projected - return the id
			return i;
		}
	}
	// row id is not yet projected - push it and return the new index
	column_ids.push_back(ColumnIndex(COLUMN_IDENTIFIER_ROW_ID));
	if (!get.projection_ids.empty()) {
		get.projection_ids.push_back(column_ids.size() - 1);
	}
	if (!get.types.empty()) {
		get.types.push_back(get.GetRowIdType());
	}
	return column_ids.size() - 1;
}

unique_ptr<LogicalGet> LateMaterialization::ConstructLHS(LogicalGet &get) {
	// we need to construct a new scan of the same table
	auto table_index = optimizer.binder.GenerateTableIndex();
	auto new_get = make_uniq<LogicalGet>(table_index, get.function, get.bind_data->Copy(), get.returned_types,
	                                     get.names, get.GetRowIdType());
	new_get->GetMutableColumnIds() = get.GetColumnIds();
	new_get->projection_ids = get.projection_ids;
	return new_get;
}

ColumnBinding LateMaterialization::ConstructRHS(unique_ptr<LogicalOperator> &op) {
	// traverse down until we reach the LogicalGet
	vector<reference<LogicalOperator>> stack;
	reference<LogicalOperator> child = *op->children[0];
	while (child.get().type != LogicalOperatorType::LOGICAL_GET) {
		stack.push_back(child);
		D_ASSERT(child.get().children.size() == 1);
		child = *child.get().children[0];
	}
	// we have reached the logical get - now we need to push the row-id column (if it is not yet projected out)
	auto &get = child.get().Cast<LogicalGet>();
	auto row_id_idx = GetOrInsertRowId(get);

	auto new_index = optimizer.binder.GenerateTableIndex();
	get.table_index = new_index;

	// the row id has been projected - now project it up the stack
	ColumnBinding row_id_binding(new_index, row_id_idx);
	for (idx_t i = stack.size(); i > 0; i--) {
		auto &op = stack[i - 1].get();
		if (row_id_binding.table_index == new_index) {
			for (auto &expr : op.expressions) {
				ReplaceTableReferences(*expr, new_index);
			}
		}
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &proj = op.Cast<LogicalProjection>();
			// push a projection of the row-id column
			proj.expressions.push_back(
			    make_uniq<BoundColumnRefExpression>("rowid", get.GetRowIdType(), row_id_binding));
			// modify the row-id-binding to push to the new projection
			row_id_binding = ColumnBinding(proj.table_index, proj.expressions.size() - 1);
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER:
			// column bindings pass-through this operator as-is
			break;
		default:
			throw InternalException("Unsupported logical operator in LateMaterialization::ConstructRHS");
		}
	}
	if (row_id_binding.table_index == new_index && op->type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &top_n = op->Cast<LogicalTopN>();
		;
		for (auto &order : top_n.orders) {
			ReplaceTableReferences(*order.expression, new_index);
		}
	}
	return row_id_binding;
}

void LateMaterialization::ReplaceTableReferences(Expression &expr, idx_t new_table_index) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_column_ref = expr.Cast<BoundColumnRefExpression>();
		bound_column_ref.binding.table_index = new_table_index;
	}

	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](Expression &child) { ReplaceTableReferences(child, new_table_index); });
}

bool LateMaterialization::TryLateMaterialization(unique_ptr<LogicalOperator> &op) {
	// check if we can benefit from late materialization
	// we need to see how many columns we require in the pipeline versus how many columns we emit in the scan
	// for example, in a query like SELECT * FROM tbl ORDER BY ts LIMIT 5, the top-n only needs the "ts" column
	// the other columns can be fetched later on using late materialization
	// we can only push late materialization through a subset of operators
	// and we can only do it for scans that support the row-id pushdown (currently only DuckDB table scans)

	// visit the expressions for each operator in the chain
	VisitOperatorExpressions(*op);
	reference<LogicalOperator> child = *op->children[0];
	while (child.get().type != LogicalOperatorType::LOGICAL_GET) {
		switch (child.get().type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			// recurse into the child node - but ONLY visit expressions that are referenced
			auto &proj = child.get().Cast<LogicalProjection>();

			// figure out which projection expressions we are currently referencing
			set<idx_t> referenced_columns;
			for (auto &entry : column_references) {
				auto &column_binding = entry.first;
				if (column_binding.table_index == proj.table_index) {
					referenced_columns.insert(column_binding.column_index);
				}
			}
			// clear the list of referenced expressions and visit those columns
			column_references.clear();
			for (auto &col_idx : referenced_columns) {
				VisitExpression(&proj.expressions[col_idx]);
			}
			// continue into child
			child = *child.get().children[0];
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER:
			// visit filter expressions - we need these columns
			VisitOperatorExpressions(child.get());
			// continue into child
			child = *child.get().children[0];
			break;
		default:
			// unsupported operator for late materialization
			return false;
		}
	}
	auto &get = child.get().Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table) {
		// we can only do the late-materialization optimization for DuckDB tables currently
		return false;
	}
	if (column_references.size() >= get.GetColumnIds().size()) {
		// we do not benefit from late materialization
		// we need all of the columns to compute the root node anyway (Top-N/Limit/etc)
		return false;
	}
	// we benefit from late materialization
	// we need to transform this plan into a semi-join with the row-id
	// we need to ensure the operator returns exactly the same column bindings as before

	// the final table index emitted must be the table index of the original get
	// this ensures any upstream operators that refer to the original get will keep on referring to the correct columns
	auto final_index = get.table_index;

	// construct the LHS from the LogicalGet
	auto lhs = ConstructLHS(get);
	// insert the row-id column on the left hand side
	auto &lhs_get = *lhs;
	auto lhs_index = lhs_get.table_index;
	auto lhs_columns = lhs_get.GetColumnIds().size();
	auto lhs_row_idx = GetOrInsertRowId(lhs_get);
	ColumnBinding lhs_binding(lhs_index, lhs_row_idx);

	auto &row_id_type = get.GetRowIdType();

	// we need to re-order again at the end
	vector<BoundOrderByNode> final_orders;
	if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
		// for top-n we need to re-order by the top-n conditions
		auto &top_n = op->Cast<LogicalTopN>();
		for (auto &order : top_n.orders) {
			auto expr = order.expression->Copy();
			ReplaceTableReferences(*expr, lhs_index);

			final_orders.emplace_back(order.type, order.null_order, std::move(expr));
		}
	} else {
		// for limit/sample we order by row-id
		auto row_id_expr = make_uniq<BoundColumnRefExpression>("rowid", row_id_type, lhs_binding);
		final_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(row_id_expr));
	}

	// construct the RHS for the join
	// this is essentially the old pipeline, but with the `rowid` column added
	// note that the purpose of this optimization is to remove columns from the RHS
	// we don't do that here yet though - we do this in a later step using the RemoveUnusedColumns optimizer
	auto rhs_binding = ConstructRHS(op);

	// construct a semi join between the lhs and rhs
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);
	join->children.push_back(std::move(lhs));
	join->children.push_back(std::move(op));
	JoinCondition condition;
	condition.comparison = ExpressionType::COMPARE_EQUAL;
	condition.left = make_uniq<BoundColumnRefExpression>("rowid", row_id_type, lhs_binding);
	condition.right = make_uniq<BoundColumnRefExpression>("rowid", row_id_type, rhs_binding);
	join->conditions.push_back(std::move(condition));

	// push a projection that removes the row id again from the lhs
	// this is the final projection - so it should have the final table index
	auto proj_index = final_index;
	vector<unique_ptr<Expression>> proj_list;
	for (idx_t i = 0; i < lhs_columns; i++) {
		auto &column_id = lhs_get.GetColumnIds()[i];
		auto is_row_id = column_id.IsRowIdColumn();
		auto column_name = is_row_id ? "rowid" : lhs_get.names[column_id.GetPrimaryIndex()];
		auto &column_type = is_row_id ? row_id_type : lhs_get.returned_types[column_id.GetPrimaryIndex()];
		proj_list.push_back(make_uniq<BoundColumnRefExpression>(column_name, column_type, ColumnBinding(lhs_index, i)));
	}
	auto order = make_uniq<LogicalOrder>(std::move(final_orders));
	order->children.push_back(std::move(join));

	auto proj = make_uniq<LogicalProjection>(proj_index, std::move(proj_list));
	proj->children.push_back(std::move(order));

	// run the RemoveUnusedColumns optimizer to prune the (now) unused columns from the RHS
	// RemoveUnusedColumns unused_optimizer(optimizer.binder, optimizer.context, true);
	// unused_optimizer.VisitOperator(*proj);

	// we have constructed the final operator - finished
	op = std::move(proj);
	return true;
}

unique_ptr<LogicalOperator> LateMaterialization::Optimize(unique_ptr<LogicalOperator> op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = op->Cast<LogicalLimit>();
		if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
			break;
		}
		if (limit.limit_val.GetConstantValue() > max_row_count) {
			break;
		}
		if (TryLateMaterialization(op)) {
			return std::move(op);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto &top_n = op->Cast<LogicalTopN>();
		if (top_n.limit > max_row_count) {
			break;
		}
		// for the top-n we need to visit the order elements
		if (TryLateMaterialization(op)) {
			return std::move(op);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_SAMPLE: {
		auto &sample = op->Cast<LogicalSample>();
		if (sample.sample_options->is_percentage) {
			break;
		}
		if (sample.sample_options->sample_size.GetValue<uint64_t>() > max_row_count) {
			break;
		}
		if (TryLateMaterialization(op)) {
			return std::move(op);
		}
		break;
	}
	default:
		break;
	}
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	return op;
}

} // namespace duckdb
