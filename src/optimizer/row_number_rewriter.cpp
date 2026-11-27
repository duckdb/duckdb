#include "duckdb/optimizer/row_number_rewriter.hpp"

#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

bool RowNumberRewriter::CanOptimize(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_WINDOW) {
		return false;
	}
	if (op.expressions.size() != 1) {
		return false;
	}

	//	If we have window fusion enabled, then we are also filtering on the row number,
	//	which reduces the number of scanned rows. This is faster than scanning them all
	//	and discarding most of them.
	auto &window = op.Cast<LogicalWindow>();
	if (window.partition_limit.IsValid()) {
		return false;
	}

	auto &expression = op.expressions[0];
	if (expression->GetExpressionType() != ExpressionType::WINDOW_ROW_NUMBER) {
		return false;
	}
	auto &window_expr = expression->Cast<BoundWindowExpression>();
	if (!window_expr.Partitions().empty() || !window_expr.OrderBy().empty()) {
		return false;
	}

	// Should be followed by a get
	auto &window_ch = op.children[0];
	if (window_ch->type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}

	auto &get = window_ch->Cast<LogicalGet>();

	// the get must support the row_number virtual column
	if (get.virtual_columns.find(COLUMN_IDENTIFIER_ROW_NUMBER) == get.virtual_columns.end()) {
		return false;
	}

	// only rewrite for a base-table scan: its virtual row_number column has SQL
	// window semantics (dense, over the visible rows of this scan). Table
	// functions such as read_duckdb also expose a row_number virtual column, but
	// with different (e.g. file-local) semantics, so mapping ROW_NUMBER() OVER ()
	// onto it would return the wrong values (issue #23586).
	if (!get.GetTable()) {
		return false;
	}

	// cannot rewrite if there are table filters pushed into the scan
	// the virtual row_number column counts absolute row positions in storage,
	// which does not match row_number() over() when rows are filtered out
	if (get.table_filters.HasFilters()) {
		return false;
	}

	return true;
}

unique_ptr<LogicalOperator> RowNumberRewriter::Optimize(unique_ptr<LogicalOperator> op) {
	BindingReplacementGraph replacements;
	op = RewritePlan(std::move(op), replacements);

	if (!replacements.Empty()) {
		ColumnBindingReplacer replacer(replacements);
		replacer.VisitOperator(*op);
	}

	return op;
}

unique_ptr<LogicalOperator> RowNumberRewriter::RewriteGet(unique_ptr<LogicalOperator> op,
                                                          BindingReplacementGraph &replacements) {
	auto &window = op->Cast<LogicalWindow>();
	auto &child = window.children[0];
	auto &get = child->Cast<LogicalGet>();

	// Add virtual row_number column to the LogicalGet
	auto proj_index = get.AddColumnId(COLUMN_IDENTIFIER_ROW_NUMBER);
	if (!get.projection_ids.empty()) {
		get.projection_ids.push_back(proj_index);
	}
	get.types.push_back(LogicalType::BIGINT);

	// Existing scan bindings stay unchanged; only the removed window output needs replacing.
	replacements.Add(ColumnBinding(window.window_index, ProjectionIndex(0)),
	                 ColumnBinding(get.table_index, proj_index));
	return std::move(window.children[0]);
}

unique_ptr<LogicalOperator> RowNumberRewriter::RewritePlan(unique_ptr<LogicalOperator> op,
                                                           BindingReplacementGraph &replacements) {
	if (CanOptimize(*op)) {
		return RewriteGet(std::move(op), replacements);
	}

	// Recurse into children
	for (auto &child : op->children) {
		child = RewritePlan(std::move(child), replacements);
	}
	return op;
}

} // namespace duckdb
