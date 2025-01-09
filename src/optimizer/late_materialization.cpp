#include "duckdb/optimizer/late_materialization.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

LateMaterialization::LateMaterialization(Optimizer &optimizer) : optimizer(optimizer) {}

ColumnBinding LateMaterialization::ConstructRHS(unique_ptr<LogicalOperator> &op) {
	// traverse down until we reach the LogicalGet
	vector<reference<LogicalOperator>> stack;
	reference<LogicalOperator> child = *op->children[0];
	while(child.get().type != LogicalOperatorType::LOGICAL_GET) {
		stack.push_back(child);
		D_ASSERT(child.get().children.size() == 1);
		child = *child.get().children[0];
	}
	// we have reached the logical get - now we need to push the row-id column (if it is not yet projected out)
	auto &get = child.get().Cast<LogicalGet>();
	auto &column_ids = get.GetMutableColumnIds();
	// check if it is already projected
	optional_idx row_id_idx;
	for(idx_t i = 0; i < column_ids.size(); ++i) {
		if (column_ids[i].IsRowIdColumn()) {
			row_id_idx = i;
			break;
		}
	}
	if (!row_id_idx.IsValid()) {
		// row id is not yet projected - push it
		row_id_idx = column_ids.size();
		column_ids.push_back(ColumnIndex(COLUMN_IDENTIFIER_ROW_ID));
	}
	// the row id has been projected - now project it up the stack
	ColumnBinding row_id_binding(get.table_index, row_id_idx.GetIndex());
	for(idx_t i = stack.size(); i > 0; i--) {
		auto &op = stack[i - 1].get();
		switch(op.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &proj = op.Cast<LogicalProjection>();
			// push a projection of the row-id column
			// FIXME: get.GetRowIdType()
			proj.expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::ROW_TYPE, row_id_binding));
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
	return row_id_binding;
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
	while(child.get().type != LogicalOperatorType::LOGICAL_GET) {
		switch(child.get().type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			// recurse into the child node - but ONLY visit expressions that are referenced
			auto &proj = child.get().Cast<LogicalProjection>();

			// figure out which projection expressions we are currently referencing
			set<idx_t> referenced_columns;
			for(auto &entry : column_references) {
				auto &column_binding = entry.first;
				if (column_binding.table_index == proj.table_index) {
					referenced_columns.insert(column_binding.column_index);
				}
			}
			// clear the list of referenced expressions and visit those columns
			column_references.clear();
			for(auto &col_idx : referenced_columns) {
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
	if (!get.projection_ids.empty()) {
		// FIXME: not supporting projection_ids just yet
		return false;
	}
	// we benefit from late materialization
	// we need to transform this plan into a semi-join with the row-id
	// we need to ensure the operator returns exactly the same column bindings as before

	// construct the LHS
	// this is essentially the old LogicalGet, but with rowid added


	// construct the RHS for the join
	// this is essentially the old pipeline, but with the `rowid` column added
	// note that the purpose of this optimization is to remove columns from the RHS
	// we don't do that here yet though - we do this in a later step using the RemoveUnusedColumns optimizer
	auto rhs_binding = ConstructRHS(op);

	// finally we run the RemoveUnusedColumns optimizer to prune the (now) unused columns from the RHS

	// finally construct the semi-join
	throw InternalException("FIXME: Late materialization");
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
		if (!sample.sample_options->is_percentage) {
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
