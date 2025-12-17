#include "duckdb/optimizer/late_materialization.hpp"

#include "duckdb/optimizer/late_materialization_helper.hpp"
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
#include "duckdb/main/settings.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

// Debug logging macros for late materialization optimization
#ifdef DEBUG
#define LATE_MAT_LOG(msg, ...) fprintf(stderr, "[LateMaterialization] " msg "\n", ##__VA_ARGS__)
#else
#define LATE_MAT_LOG(msg, ...)
#endif

namespace duckdb {

LateMaterialization::LateMaterialization(Optimizer &optimizer) : optimizer(optimizer) {
	max_row_count = DBConfig::GetSetting<LateMaterializationMaxRowsSetting>(optimizer.context);
}

vector<ColumnBinding> LateMaterialization::ConstructRHS(unique_ptr<LogicalOperator> &op) {
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
	auto row_id_indexes = LateMaterializationHelper::GetOrInsertRowIds(get, row_id_column_ids, row_id_columns);
	idx_t column_count = get.projection_ids.empty() ? get.GetColumnIds().size() : get.projection_ids.size();
	D_ASSERT(column_count == get.GetColumnBindings().size());

	// the row id has been projected - now project it up the stack
	vector<ColumnBinding> row_id_bindings;
	for (auto &row_id_index : row_id_indexes) {
		row_id_bindings.emplace_back(get.table_index, row_id_index);
	}
	for (idx_t i = stack.size(); i > 0; i--) {
		auto &op = stack[i - 1].get();
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &proj = op.Cast<LogicalProjection>();
			// push projection of the row-id columns
			for (idx_t r_idx = 0; r_idx < row_id_columns.size(); r_idx++) {
				auto &r_col = row_id_columns[r_idx];
				proj.expressions.push_back(
				    make_uniq<BoundColumnRefExpression>(r_col.name, r_col.type, row_id_bindings[r_idx]));
				// modify the row-id-binding to the new projection
				row_id_bindings[r_idx] = ColumnBinding(proj.table_index, proj.expressions.size() - 1);
			}
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
	return row_id_bindings;
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
				ReplaceTableReferences(order.expression, new_index);
			}
			current_op = *op.children[0];
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_SAMPLE:
		case LogicalOperatorType::LOGICAL_LIMIT: {
			// visit the expressions of the operator and continue into the child node
			for (auto &expr : op.expressions) {
				ReplaceTableReferences(expr, new_index);
			}
			current_op = *op.children[0];
			break;
		}
		default:
			throw InternalException("Unsupported operator type in LateMaterialization::ReplaceTopLevelTableIndex");
		}
	}
}

void LateMaterialization::ReplaceTableReferences(unique_ptr<Expression> &root_expr, idx_t new_table_index) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    root_expr, [&](BoundColumnRefExpression &bound_column_ref, unique_ptr<Expression> &expr) {
		    bound_column_ref.binding.table_index = new_table_index;
	    });
}

unique_ptr<Expression> LateMaterialization::GetExpression(LogicalOperator &op, idx_t column_index) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		auto &column_id = get.GetColumnIds()[column_index];
		auto column_name = get.GetColumnName(column_id);
		auto &column_type = get.GetColumnType(column_id);
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

void LateMaterialization::ReplaceExpressionReferences(LogicalOperator &next_op, unique_ptr<Expression> &root_expr) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    root_expr, [&](BoundColumnRefExpression &bound_column_ref, unique_ptr<Expression> &expr) {
		    expr = GetExpression(next_op, bound_column_ref.binding.column_index);
	    });
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
	if (column_references.size() >= get.GetColumnIds().size()) {
		// we do not benefit from late materialization
		// we need all of the columns to compute the root node anyway (Top-N/Limit/etc)
		return false;
	}
	if (!get.function.late_materialization) {
		// this function does not support late materialization
		return false;
	}
	if (!get.function.get_row_id_columns) {
		throw InternalException("Function supports late materialization but not get_row_id_columns");
	}
	row_id_column_ids = get.function.get_row_id_columns(optimizer.context, get.bind_data.get());
	if (row_id_column_ids.empty()) {
		throw InternalException("Row Id Columns must not be empty");
	}
	row_id_columns.clear();
	for (auto &col_id : row_id_column_ids) {
		auto entry = get.virtual_columns.find(col_id);
		if (entry == get.virtual_columns.end()) {
			throw InternalException("Row id column id not found in virtual column list");
		}
		row_id_columns.push_back(entry->second);
	}
	// we benefit from late materialization
	// we need to transform this plan into a semi-join with the row-id
	// we need to ensure the operator returns exactly the same column bindings as before

	// construct the LHS from the LogicalGet
	auto lhs = LateMaterializationHelper::CreateLHSGet(get, optimizer.binder);
	// insert the row-id column on the left hand side
	auto &lhs_get = *lhs;
	auto lhs_index = lhs_get.table_index;
	auto lhs_columns = lhs_get.GetColumnIds().size();
	auto lhs_row_indexes = LateMaterializationHelper::GetOrInsertRowIds(lhs_get, row_id_column_ids, row_id_columns);
	vector<ColumnBinding> lhs_bindings;
	for (auto &lhs_row_index : lhs_row_indexes) {
		lhs_bindings.emplace_back(lhs_index, lhs_row_index);
	}

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
		for (idx_t r_idx = 0; r_idx < row_id_columns.size(); r_idx++) {
			auto &row_id_col = row_id_columns[r_idx];
			auto row_id_expr =
			    make_uniq<BoundColumnRefExpression>(row_id_col.name, row_id_col.type, lhs_bindings[r_idx]);
			final_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(row_id_expr));
		}
	}

	// construct the RHS for the join
	// this is essentially the old pipeline, but with the `rowid` column added
	// note that the purpose of this optimization is to remove columns from the RHS
	// we don't do that here yet though - we do this in a later step using the RemoveUnusedColumns optimizer
	auto rhs_bindings = ConstructRHS(op);

	// the final table index emitted must be the table index of the original operator
	// this ensures any upstream operators that refer to the original get will keep on referring to the correct columns
	auto final_index = rhs_bindings[0].table_index;

	// we need to replace any references to "rhs_binding.table_index" in the rhs to a new table index
	auto rhs_table_index = optimizer.binder.GenerateTableIndex();
	for (auto &rhs_binding : rhs_bindings) {
		rhs_binding.table_index = rhs_table_index;
	}
	ReplaceTopLevelTableIndex(*op, rhs_table_index);
	if (rhs_bindings.size() != lhs_bindings.size()) {
		throw InternalException("Mismatch in late materialization binding sizes");
	}

	// construct a semi join between the lhs and rhs
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);
	join->children.push_back(std::move(lhs));
	join->children.push_back(std::move(op));

	for (idx_t r_idx = 0; r_idx < row_id_columns.size(); r_idx++) {
		auto &row_id_col = row_id_columns[r_idx];
		JoinCondition condition;
		condition.comparison = ExpressionType::COMPARE_EQUAL;
		condition.left = make_uniq<BoundColumnRefExpression>(row_id_col.name, row_id_col.type, lhs_bindings[r_idx]);
		condition.right = make_uniq<BoundColumnRefExpression>(row_id_col.name, row_id_col.type, rhs_bindings[r_idx]);
		join->conditions.push_back(std::move(condition));
	}

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
			ReplaceTableReferences(order.expression, proj_index);
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

//===--------------------------------------------------------------------===//
// Semi-Join Late Materialization - Helper Functions
//===--------------------------------------------------------------------===//

bool LateMaterialization::IsExpensiveColumnType(const LogicalType &type) {
	// Consider BLOB, VARCHAR, and nested types as "expensive" to project
	switch (type.id()) {
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::LIST:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::UNION:
		return true;
	default:
		return false;
	}
}

LogicalGet *LateMaterialization::FindProbeGet(LogicalOperator &op, vector<reference<LogicalOperator>> &path) {
	// Traverse through filters and projections to find the LogicalGet
	reference<LogicalOperator> current = op;
	while (true) {
		switch (current.get().type) {
		case LogicalOperatorType::LOGICAL_GET:
			return &current.get().Cast<LogicalGet>();
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_PROJECTION:
			path.push_back(current);
			if (current.get().children.empty()) {
				return nullptr;
			}
			current = *current.get().children[0];
			break;
		default:
			// Unsupported operator type in path
			return nullptr;
		}
	}
}

column_binding_set_t LateMaterialization::GetJoinConditionBindings(LogicalComparisonJoin &join) {
	column_binding_set_t bindings;
	for (auto &condition : join.conditions) {
		// Extract column bindings from the left side of the condition (probe side)
		ExpressionIterator::EnumerateExpression(condition.left, [&](Expression &expr) {
			if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				auto &colref = expr.Cast<BoundColumnRefExpression>();
				bindings.insert(colref.binding);
			}
		});
	}
	return bindings;
}

bool LateMaterialization::HasExpensiveNonJoinColumns(LogicalGet &get, const column_binding_set_t &join_bindings,
                                                     vector<idx_t> &expensive_column_indices) {
	expensive_column_indices.clear();
	auto &column_ids = get.GetColumnIds();

	LATE_MAT_LOG("HasExpensiveNonJoinColumns: checking %llu columns", static_cast<unsigned long long>(column_ids.size()));

	for (idx_t i = 0; i < column_ids.size(); i++) {
		ColumnBinding binding(get.table_index, i);

		// Check if this column is used in the join condition
		bool in_join = join_bindings.find(binding) != join_bindings.end();
		
		// Get the column type using the column_id, not the index
		auto &col_id = column_ids[i];
		string col_name = get.GetColumnName(col_id);
		auto &column_type = get.GetColumnType(col_id);
		
		LATE_MAT_LOG("  Column %llu: name='%s', type='%s', binding=(%llu,%llu), in_join=%d, is_expensive=%d",
		             static_cast<unsigned long long>(i),
		             col_name.c_str(), column_type.ToString().c_str(),
		             static_cast<unsigned long long>(binding.table_index),
		             static_cast<unsigned long long>(binding.column_index),
		             in_join, IsExpensiveColumnType(column_type));
		
		if (in_join) {
			continue; // This column is needed for the join
		}

		// Check if this column type is expensive
		if (IsExpensiveColumnType(column_type)) {
			expensive_column_indices.push_back(i);
			LATE_MAT_LOG("  -> Found expensive non-join column at index %llu", static_cast<unsigned long long>(i));
		}
	}

	LATE_MAT_LOG("HasExpensiveNonJoinColumns: found %llu expensive columns", 
	             static_cast<unsigned long long>(expensive_column_indices.size()));
	return !expensive_column_indices.empty();
}

//===--------------------------------------------------------------------===//
// TryLateMaterializationSemiJoin
//===--------------------------------------------------------------------===//

bool LateMaterialization::TryLateMaterializationSemiJoin(unique_ptr<LogicalOperator> &op) {
	// This method handles semi-joins where the probe side has expensive columns
	// that are not needed for the join condition.
	//
	// Original plan:
	//   PROJECTION [item_id, large_blob]
	//     SEMI_JOIN (item_id = id)
	//       TABLE_SCAN [item_id, large_blob, timestamp]  <- expensive blob projected for all rows
	//       (build side)
	//
	// Transformed plan:
	//   PROJECTION [item_id, large_blob]
	//     INNER_JOIN (rowid = rowid)  <- late fetch join
	//       TABLE_SCAN [item_id, large_blob, rowid]  <- fetch scan (for output)
	//       SEMI_JOIN (item_id = id)
	//         TABLE_SCAN [item_id, rowid, timestamp]  <- filtered scan (no blob)
	//         (build side)

	auto &join = op->Cast<LogicalComparisonJoin>();
	LATE_MAT_LOG("Examining semi-join for late materialization, join_type=%d", static_cast<int>(join.join_type));

	// Only handle SEMI and ANTI joins
	if (join.join_type != JoinType::SEMI && join.join_type != JoinType::ANTI) {
		LATE_MAT_LOG("Not a SEMI/ANTI join, skipping");
		return false;
	}

	// Find the LogicalGet on the probe side (left child)
	vector<reference<LogicalOperator>> probe_path;
	auto probe_get = FindProbeGet(*join.children[0], probe_path);
	if (!probe_get) {
		LATE_MAT_LOG("Could not find LogicalGet on probe side");
		return false;
	}

	LATE_MAT_LOG("Found probe LogicalGet with table_index=%llu, %zu columns, probe_path size=%zu",
	             static_cast<unsigned long long>(probe_get->table_index), probe_get->GetColumnIds().size(),
	             probe_path.size());


	// Check if the table supports late materialization
	if (!probe_get->function.late_materialization) {
		LATE_MAT_LOG("Table function does not support late materialization");
		return false;
	}

	if (!probe_get->function.get_row_id_columns) {
		LATE_MAT_LOG("Table function does not support get_row_id_columns");
		return false;
	}

	// Get the column bindings used in join conditions
	auto join_bindings = GetJoinConditionBindings(join);
	LATE_MAT_LOG("Join condition uses %zu column bindings", join_bindings.size());

	// Check if there are expensive columns not used in the join
	vector<idx_t> expensive_columns;
	if (!HasExpensiveNonJoinColumns(*probe_get, join_bindings, expensive_columns)) {
		LATE_MAT_LOG("No expensive non-join columns found, skipping optimization");
		return false;
	}

	LATE_MAT_LOG("Found %zu expensive columns to defer", expensive_columns.size());

	// Get row-id column information
	row_id_column_ids = probe_get->function.get_row_id_columns(optimizer.context, probe_get->bind_data.get());
	if (row_id_column_ids.empty()) {
		LATE_MAT_LOG("Row ID columns are empty, cannot proceed");
		return false;
	}

	row_id_columns.clear();
	for (auto &col_id : row_id_column_ids) {
		auto entry = probe_get->virtual_columns.find(col_id);
		if (entry == probe_get->virtual_columns.end()) {
			LATE_MAT_LOG("Row id column id %llu not found in virtual column list",
			             static_cast<unsigned long long>(col_id));
			return false;
		}
		row_id_columns.push_back(entry->second);
	}

	LATE_MAT_LOG("Using %zu row-id columns for late materialization", row_id_columns.size());

	// === Construct the transformation ===

	// 1. Create the "fetch" scan (LHS of late-fetch join) - contains all original columns + rowid
	auto fetch_get = LateMaterializationHelper::CreateLHSGet(*probe_get, optimizer.binder);
	auto fetch_index = fetch_get->table_index;
	auto fetch_row_indexes = LateMaterializationHelper::GetOrInsertRowIds(*fetch_get, row_id_column_ids, row_id_columns);

	LATE_MAT_LOG("Created fetch scan with table_index=%llu", static_cast<unsigned long long>(fetch_index));

	vector<ColumnBinding> fetch_rowid_bindings;
	for (auto &idx : fetch_row_indexes) {
		fetch_rowid_bindings.emplace_back(fetch_index, idx);
	}

	// 2. Add rowid to the probe side (the existing probe_get)
	auto probe_row_indexes =
	    LateMaterializationHelper::GetOrInsertRowIds(*probe_get, row_id_column_ids, row_id_columns);

	LATE_MAT_LOG("Added rowid to probe scan, rowid index=%llu", static_cast<unsigned long long>(probe_row_indexes[0]));

	// Determine the "top-level" operator of the probe side - this is what the semi-join outputs
	// If probe_path is empty, it's probe_get. Otherwise it's the first operator in probe_path.
	idx_t original_top_index;
	if (probe_path.empty()) {
		original_top_index = probe_get->table_index;
	} else {
		// The first operator in probe_path is closest to the semi-join (outermost)
		auto &top_op = probe_path[0].get();
		if (top_op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			original_top_index = top_op.Cast<LogicalProjection>().table_index;
		} else {
			// Filters pass through the bindings from their child
			// So we need to look at what the filter outputs
			original_top_index = probe_get->table_index;
		}
	}

	// Give probe_get a NEW table_index to avoid conflict with final_proj
	auto new_probe_index = optimizer.binder.GenerateTableIndex();
	auto old_probe_index = probe_get->table_index;
	probe_get->table_index = new_probe_index;

	LATE_MAT_LOG("Changed probe_get table_index from %llu to %llu",
	             static_cast<unsigned long long>(old_probe_index),
	             static_cast<unsigned long long>(new_probe_index));

	// Update intermediate operators to reference the new probe_get table_index
	// Process path from innermost (closest to GET) to outermost (closest to JOIN)
	idx_t column_count = probe_get->projection_ids.empty() ? probe_get->GetColumnIds().size()
	                                                       : probe_get->projection_ids.size();
	
	// Keep track of what table_index the top-level operator will have after our modifications
	idx_t new_top_index = new_probe_index;

	for (idx_t path_idx = probe_path.size(); path_idx > 0; path_idx--) {
		auto &path_op = probe_path[path_idx - 1].get();
		if (path_op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &proj = path_op.Cast<LogicalProjection>();
			
			// Update projection expressions to reference the new child table_index
			for (auto &expr : proj.expressions) {
				ReplaceTableReferences(expr, new_top_index);
			}
			
			// Give this projection a new table_index
			auto old_proj_index = proj.table_index;
			auto new_proj_index = optimizer.binder.GenerateTableIndex();
			proj.table_index = new_proj_index;
			new_top_index = new_proj_index;
			
			column_count = proj.expressions.size();
			
			LATE_MAT_LOG("Updated projection table_index from %llu to %llu",
			             static_cast<unsigned long long>(old_proj_index),
			             static_cast<unsigned long long>(new_proj_index));
		} else if (path_op.type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = path_op.Cast<LogicalFilter>();
			// Update filter expressions to reference the new child table_index
			for (auto &expr : filter.expressions) {
				ReplaceTableReferences(expr, new_top_index);
			}
			// Filters pass through bindings, so new_top_index stays the same
			LATE_MAT_LOG("Updated filter to reference table_index %llu",
			             static_cast<unsigned long long>(new_top_index));
		}
	}

	// Update semi-join conditions to reference the new top-level table index
	for (auto &condition : join.conditions) {
		ReplaceTableReferences(condition.left, new_top_index);
	}

	// probe_rowid_bindings reference the top-level output of the probe side
	// This is probe_get if probe_path is empty, otherwise we need to project rowid through the path
	vector<ColumnBinding> probe_rowid_bindings;
	for (auto &idx : probe_row_indexes) {
		probe_rowid_bindings.emplace_back(new_probe_index, idx);
	}

	// Now project rowid through the intermediate operators
	for (idx_t path_idx = probe_path.size(); path_idx > 0; path_idx--) {
		auto &path_op = probe_path[path_idx - 1].get();
		if (path_op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &proj = path_op.Cast<LogicalProjection>();
			// Add rowid expressions to the projection
			for (idx_t r_idx = 0; r_idx < row_id_columns.size(); r_idx++) {
				auto &r_col = row_id_columns[r_idx];
				proj.expressions.push_back(
				    make_uniq<BoundColumnRefExpression>(r_col.name, r_col.type, probe_rowid_bindings[r_idx]));
				// Update binding to reference the projection's output
				probe_rowid_bindings[r_idx] = ColumnBinding(proj.table_index, proj.expressions.size() - 1);
			}
			column_count = proj.expressions.size();
			LATE_MAT_LOG("Projected rowid through projection, new binding: (%llu, %llu)",
			             static_cast<unsigned long long>(probe_rowid_bindings[0].table_index),
			             static_cast<unsigned long long>(probe_rowid_bindings[0].column_index));
		} else if (path_op.type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = path_op.Cast<LogicalFilter>();
			if (filter.HasProjectionMap()) {
				// Add rowid to the filter's projection map
				filter.projection_map.push_back(column_count - 1);
			}
			// Filters pass through bindings unchanged
		}
	}

	LATE_MAT_LOG("Final probe rowid binding: (%llu, %llu)",
	             static_cast<unsigned long long>(probe_rowid_bindings[0].table_index),
	             static_cast<unsigned long long>(probe_rowid_bindings[0].column_index));

	// Use original_top_index for the final projection output
	auto original_probe_index = original_top_index;

	// 3. Create a projection above the semi-join that explicitly passes rowid
	// This ensures RemoveUnusedColumns sees the direct reference
	auto rhs_wrapper_index = optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> rhs_wrapper_exprs;
	
	// Add all the original semi-join output columns (from probe side) plus rowid at the end
	// For now, just add the rowid expression - RemoveUnusedColumns will prune unused columns
	for (idx_t r_idx = 0; r_idx < row_id_columns.size(); r_idx++) {
		auto &r_col = row_id_columns[r_idx];
		rhs_wrapper_exprs.push_back(
		    make_uniq<BoundColumnRefExpression>(r_col.name, r_col.type, probe_rowid_bindings[r_idx]));
	}
	
	auto rhs_wrapper_proj = make_uniq<LogicalProjection>(rhs_wrapper_index, std::move(rhs_wrapper_exprs));
	if (op->has_estimated_cardinality) {
		rhs_wrapper_proj->SetEstimatedCardinality(op->estimated_cardinality);
	}
	rhs_wrapper_proj->children.push_back(std::move(op));
	
	// Update the probe rowid bindings to reference the wrapper projection
	vector<ColumnBinding> wrapper_rowid_bindings;
	for (idx_t r_idx = 0; r_idx < row_id_columns.size(); r_idx++) {
		wrapper_rowid_bindings.emplace_back(rhs_wrapper_index, r_idx);
	}
	
	LATE_MAT_LOG("Created RHS wrapper projection with table_index=%llu, rowid at binding (%llu, 0)",
	             static_cast<unsigned long long>(rhs_wrapper_index),
	             static_cast<unsigned long long>(rhs_wrapper_index));

	// 4. Create the late-fetch INNER join on rowid
	auto late_fetch_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);

	for (idx_t r_idx = 0; r_idx < row_id_columns.size(); r_idx++) {
		auto &row_id_col = row_id_columns[r_idx];
		JoinCondition condition;
		condition.comparison = ExpressionType::COMPARE_EQUAL;
		condition.left =
		    make_uniq<BoundColumnRefExpression>(row_id_col.name, row_id_col.type, fetch_rowid_bindings[r_idx]);
		condition.right =
		    make_uniq<BoundColumnRefExpression>(row_id_col.name, row_id_col.type, wrapper_rowid_bindings[r_idx]);
		late_fetch_join->conditions.push_back(std::move(condition));
	}

	if (rhs_wrapper_proj->has_estimated_cardinality) {
		late_fetch_join->SetEstimatedCardinality(rhs_wrapper_proj->estimated_cardinality);
	}

	// 5. Wire up the plan:
	// late_fetch_join->children[0] = fetch_get (LHS - contains all columns)
	// late_fetch_join->children[1] = rhs_wrapper_proj (RHS - projects just rowid from semi-join)
	late_fetch_join->children.push_back(std::move(fetch_get));
	late_fetch_join->children.push_back(std::move(rhs_wrapper_proj));

	// 6. Create final projection to restore original column bindings
	// The output should reference the fetch_get columns (not the rowid)
	auto final_proj_index = original_probe_index;
	vector<unique_ptr<Expression>> final_proj_list;

	auto &fetch_get_ref = late_fetch_join->children[0]->Cast<LogicalGet>();
	auto &fetch_column_ids = fetch_get_ref.GetColumnIds();
	idx_t fetch_num_cols = fetch_get_ref.projection_ids.empty() ? fetch_column_ids.size()
	                                                            : fetch_get_ref.projection_ids.size();

	// Project all non-rowid columns from the fetch get
	for (idx_t i = 0; i < fetch_num_cols; i++) {
		// Skip rowid columns
		bool is_rowid = false;
		for (auto &rowid_idx : fetch_row_indexes) {
			if (i == rowid_idx) {
				is_rowid = true;
				break;
			}
		}
		if (is_rowid) {
			continue;
		}

		auto &col_id = fetch_column_ids[i];
		auto &col_type = fetch_get_ref.returned_types[i];
		string col_name = fetch_get_ref.GetColumnName(col_id);
		auto col_expr = make_uniq<BoundColumnRefExpression>(col_name, col_type, ColumnBinding(fetch_index, i));
		final_proj_list.push_back(std::move(col_expr));
	}

	auto final_proj = make_uniq<LogicalProjection>(final_proj_index, std::move(final_proj_list));
	if (late_fetch_join->has_estimated_cardinality) {
		final_proj->SetEstimatedCardinality(late_fetch_join->estimated_cardinality);
	}
	final_proj->children.push_back(std::move(late_fetch_join));

	op = std::move(final_proj);

	LATE_MAT_LOG("Successfully applied semi-join late materialization");

	// Run RemoveUnusedColumns with everything_referenced=true to lock in all column bindings
	// This prevents later RemoveUnusedColumns passes from incorrectly pruning rowid columns
	// from intermediate projections in the probe path
	RemoveUnusedColumns unused_optimizer(optimizer.binder, optimizer.context, true);
	unused_optimizer.VisitOperator(*op);

	return true;
}

bool LateMaterialization::OptimizeLargeLimit(LogicalLimit &limit, idx_t limit_val, bool has_offset) {
	if (!has_offset && !DBConfig::GetSetting<PreserveInsertionOrderSetting>(optimizer.context)) {
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
	// if there are any filters we shouldn't do large limit optimization
	auto &get = current_op.get().Cast<LogicalGet>();
	if (!get.table_filters.filters.empty()) {
		return false;
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
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		// Try late materialization for semi-joins with expensive columns on probe side
		auto &join = op->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI) {
			LATE_MAT_LOG("Found SEMI/ANTI join, attempting late materialization");
			if (TryLateMaterializationSemiJoin(op)) {
				LATE_MAT_LOG("Successfully optimized semi-join with late materialization");
				// Don't recurse into children - the wrapped semi-join should not be
				// re-optimized. Subsequent optimizer passes will handle any nested
				// structures that might benefit from optimization.
				return op;
			}
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
