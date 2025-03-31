#include "duckdb/optimizer/late_materialization.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

LateMaterialization::LateMaterialization(Optimizer &optimizer) : optimizer(optimizer) {
	max_row_count = ClientConfig::GetConfig(optimizer.context).late_materialization_max_rows;
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
	idx_t column_count = get.projection_ids.empty() ? get.GetColumnIds().size() : get.projection_ids.size();
	D_ASSERT(column_count == get.GetColumnBindings().size());

	// the row id has been projected - now project it up the stack
	ColumnBinding row_id_binding(get.table_index, row_id_idx);
	for (idx_t i = stack.size(); i > 0; i--) {
		auto &op = stack[i - 1].get();
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &proj = op.Cast<LogicalProjection>();
			// push a projection of the row-id column
			proj.expressions.push_back(
			    make_uniq<BoundColumnRefExpression>("rowid", get.GetRowIdType(), row_id_binding));
			// modify the row-id-binding to push to the new projection
			row_id_binding = ColumnBinding(proj.table_index, proj.expressions.size() - 1);
			column_count = proj.expressions.size();
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto &filter = op.Cast<LogicalFilter>();
			// column bindings pass-through this operator as-is UNLESS the filter has a projection map
			if (filter.HasProjectionMap()) {
				// if the filter has a projection map, we need to project the new column
				filter.projection_map.push_back(column_count - 1);
			}
			break;
		}
		default:
			throw InternalException("Unsupported logical operator in LateMaterialization::ConstructRHS");
		}
	}
	return row_id_binding;
}

void LateMaterialization::ReplaceTopLevelTableIndex(LogicalOperator &root, idx_t new_index) {
	reference<LogicalOperator> current_op = root;
	while (true) {
		auto &op = current_op.get();
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			// reached a projection - modify the table index and return
			auto &proj = op.Cast<LogicalProjection>();
			proj.table_index = new_index;
			return;
		}
		case LogicalOperatorType::LOGICAL_GET: {
			// reached the root get - modify the table index and return
			auto &get = op.Cast<LogicalGet>();
			get.table_index = new_index;
			return;
		}
		case LogicalOperatorType::LOGICAL_TOP_N: {
			// visit the expressions of the operator and continue into the child node
			auto &top_n = op.Cast<LogicalTopN>();
			for (auto &order : top_n.orders) {
				ReplaceTableReferences(*order.expression, new_index);
			}
			current_op = *op.children[0];
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_SAMPLE:
		case LogicalOperatorType::LOGICAL_LIMIT: {
			// visit the expressions of the operator and continue into the child node
			for (auto &expr : op.expressions) {
				ReplaceTableReferences(*expr, new_index);
			}
			current_op = *op.children[0];
			break;
		}
		default:
			throw InternalException("Unsupported operator type in LateMaterialization::ReplaceTopLevelTableIndex");
		}
	}
}

void LateMaterialization::ReplaceTableReferences(Expression &expr, idx_t new_table_index) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_column_ref = expr.Cast<BoundColumnRefExpression>();
		bound_column_ref.binding.table_index = new_table_index;
	}

	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](Expression &child) { ReplaceTableReferences(child, new_table_index); });
}

unique_ptr<Expression> LateMaterialization::GetExpression(LogicalOperator &op, idx_t column_index) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		auto &column_id = get.GetColumnIds()[column_index];
		auto is_row_id = column_id.IsRowIdColumn();
		auto column_name = is_row_id ? "rowid" : get.names[column_id.GetPrimaryIndex()];
		auto &column_type = is_row_id ? get.GetRowIdType() : get.returned_types[column_id.GetPrimaryIndex()];
		auto expr =
		    make_uniq<BoundColumnRefExpression>(column_name, column_type, ColumnBinding(get.table_index, column_index));
		return std::move(expr);
	}
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return op.expressions[column_index]->Copy();
	default:
		throw InternalException("Unsupported operator type for LateMaterialization::GetExpression");
	}
}

void LateMaterialization::ReplaceExpressionReferences(LogicalOperator &next_op, unique_ptr<Expression> &expr) {
	if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_column_ref = expr->Cast<BoundColumnRefExpression>();
		expr = GetExpression(next_op, bound_column_ref.binding.column_index);
		return;
	}

	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { ReplaceExpressionReferences(next_op, child); });
}

bool LateMaterialization::TryLateMaterialization(unique_ptr<LogicalOperator> &op) {
	// check if we can benefit from late materialization
	// we need to see how many columns we require in the pipeline versus how many columns we emit in the scan
	// for example, in a query like SELECT * FROM tbl ORDER BY ts LIMIT 5, the top-n only needs the "ts" column
	// the other columns can be fetched later on using late materialization
	// we can only push late materialization through a subset of operators
	// and we can only do it for scans that support the row-id pushdown (currently only DuckDB table scans)

	// visit the expressions for each operator in the chain
	vector<reference<LogicalOperator>> source_operators;

	VisitOperatorExpressions(*op);
	reference<LogicalOperator> child = *op->children[0];
	while (child.get().type != LogicalOperatorType::LOGICAL_GET) {
		switch (child.get().type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			// recurse into the child node - but ONLY visit expressions that are referenced
			auto &proj = child.get().Cast<LogicalProjection>();
			source_operators.push_back(child);

			for (auto &expr : proj.expressions) {
				if (expr->IsVolatile()) {
					// we cannot do this optimization if any of the columns are volatile
					return false;
				}
			}

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
		case LogicalOperatorType::LOGICAL_FILTER: {
			// visit filter expressions - we need these columns
			VisitOperatorExpressions(child.get());
			// continue into child
			child = *child.get().children[0];
			break;
		}
		default:
			// unsupported operator for late materialization
			return false;
		}
	}
	auto &get = child.get().Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table || !table->IsDuckTable()) {
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

	// construct the LHS from the LogicalGet
	auto lhs = ConstructLHS(get);
	// insert the row-id column on the left hand side
	auto &lhs_get = *lhs;
	auto lhs_index = lhs_get.table_index;
	auto lhs_columns = lhs_get.GetColumnIds().size();
	auto lhs_row_idx = GetOrInsertRowId(lhs_get);
	ColumnBinding lhs_binding(lhs_index, lhs_row_idx);

	auto &row_id_type = get.GetRowIdType();

	// after constructing the LHS but before constructing the RHS we construct the final projections/orders
	// - we do this before constructing the RHS because that alter the original plan
	vector<unique_ptr<Expression>> final_proj_list;
	// construct the final projection list from either (1) the root projection, or (2) the logical get
	if (!source_operators.empty()) {
		// construct the columns from the root projection
		auto &root_proj = source_operators[0].get();
		for (auto &expr : root_proj.expressions) {
			final_proj_list.push_back(expr->Copy());
		}
		// now we need to "flatten" the projection list by traversing the set of projections and inlining them
		for (idx_t i = 0; i < source_operators.size(); i++) {
			auto &next_operator = i + 1 < source_operators.size() ? source_operators[i + 1].get() : lhs_get;
			for (auto &expr : final_proj_list) {
				ReplaceExpressionReferences(next_operator, expr);
			}
		}
	} else {
		// if we have no projection directly construct the columns from the root get
		for (idx_t i = 0; i < lhs_columns; i++) {
			final_proj_list.push_back(GetExpression(lhs_get, i));
		}
	}

	// we need to re-order again at the end
	vector<BoundOrderByNode> final_orders;
	auto root_type = op->type;
	if (root_type == LogicalOperatorType::LOGICAL_TOP_N) {
		// for top-n we need to re-order by the top-n conditions
		auto &top_n = op->Cast<LogicalTopN>();
		for (auto &order : top_n.orders) {
			auto expr = order.expression->Copy();
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

	// the final table index emitted must be the table index of the original operator
	// this ensures any upstream operators that refer to the original get will keep on referring to the correct columns
	auto final_index = rhs_binding.table_index;

	// we need to replace any references to "rhs_binding.table_index" in the rhs to a new table index
	rhs_binding.table_index = optimizer.binder.GenerateTableIndex();
	ReplaceTopLevelTableIndex(*op, rhs_binding.table_index);

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
	if (root_type == LogicalOperatorType::LOGICAL_TOP_N) {
		// for top-n we need to order on expressions, so we need to order AFTER the final projection
		auto proj = make_uniq<LogicalProjection>(proj_index, std::move(final_proj_list));
		if (join->has_estimated_cardinality) {
			proj->SetEstimatedCardinality(join->estimated_cardinality);
		}
		proj->children.push_back(std::move(join));

		for (auto &order : final_orders) {
			ReplaceTableReferences(*order.expression, proj_index);
		}
		auto order = make_uniq<LogicalOrder>(std::move(final_orders));
		if (proj->has_estimated_cardinality) {
			order->SetEstimatedCardinality(proj->estimated_cardinality);
		}
		order->children.push_back(std::move(proj));

		op = std::move(order);
	} else {
		// for limit/sample we order on row-id, so we need to order BEFORE the final projection
		// because the final projection removes row-ids
		auto order = make_uniq<LogicalOrder>(std::move(final_orders));
		if (join->has_estimated_cardinality) {
			order->SetEstimatedCardinality(join->estimated_cardinality);
		}
		order->children.push_back(std::move(join));

		auto proj = make_uniq<LogicalProjection>(proj_index, std::move(final_proj_list));
		if (order->has_estimated_cardinality) {
			proj->SetEstimatedCardinality(order->estimated_cardinality);
		}
		proj->children.push_back(std::move(order));

		op = std::move(proj);
	}

	// run the RemoveUnusedColumns optimizer to prune the (now) unused columns the plan
	RemoveUnusedColumns unused_optimizer(optimizer.binder, optimizer.context, true);
	unused_optimizer.VisitOperator(*op);
	return true;
}

bool LateMaterialization::OptimizeLargeLimit(LogicalLimit &limit, idx_t limit_val, bool has_offset) {
	auto &config = DBConfig::GetConfig(optimizer.context);
	if (!has_offset && !config.options.preserve_insertion_order) {
		// we avoid optimizing large limits if preserve insertion order is false
		// since the limit is executed in parallel anyway
		return false;
	}
	// we only perform this optimization until a certain amount of maximum values to reduce memory constraints
	// since we still materialize the set of row-ids in the hash table this optimization can increase memory pressure
	// FIXME: make this configurable as well
	static constexpr const idx_t LIMIT_MAX_VAL = 1000000;
	if (limit_val > LIMIT_MAX_VAL) {
		return false;
	}
	// we only support large limits if they are directly below the source
	reference<LogicalOperator> current_op = *limit.children[0];
	while (current_op.get().type != LogicalOperatorType::LOGICAL_GET) {
		if (current_op.get().type != LogicalOperatorType::LOGICAL_PROJECTION) {
			return false;
		}
		current_op = *current_op.get().children[0];
	}
	return true;
}

unique_ptr<LogicalOperator> LateMaterialization::Optimize(unique_ptr<LogicalOperator> op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = op->Cast<LogicalLimit>();
		if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
			break;
		}
		auto limit_val = limit.limit_val.GetConstantValue();
		bool has_offset = limit.offset_val.Type() != LimitNodeType::UNSET;
		if (limit_val > max_row_count) {
			// for large limits - we may still want to do this optimization if the limit is consecutive
			// this is the case if there are only projections/get below the limit
			// if the row-ids are not consecutive doing the join can worsen performance
			if (!OptimizeLargeLimit(limit, limit_val, has_offset)) {
				break;
			}
		} else {
			// optimizing small limits really only makes sense if we have an offset
			if (!has_offset) {
				break;
			}
		}
		if (TryLateMaterialization(op)) {
			return op;
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
			return op;
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
			return op;
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
