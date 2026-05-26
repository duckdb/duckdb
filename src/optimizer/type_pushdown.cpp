#include "duckdb/optimizer/type_pushdown.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

/*
 * This pass implements type pushdown for file readers. If a LOGICAL_PROJECTION
 * has a LOGICAL_GET as a child, and for some column that's cast in
 * LOGICAL_PROJECTION there is no other usage (including uncasted) in the query,
 * and file reader supports type pushdown, we can push down the type cast into
 * LOGICAL_GET.
 *
 * Example: SELECT ts::TIMESTAMP FROM file_reader();
 * We can push TIMESTAMP as ts's output type to file_reader();
 *
 * This pass runs before FILTER_PUSHDOWN so that WHERE conditions are still
 * visible as LOGICAL_FILTER nodes. That makes uncasted column usage detectable
 * by CollectFromOp without need to inspect table_filters.
 */

namespace duckdb {

TypePushdown::TypePushdown(ClientContext &context) : context(context) {
}

using GetCastMap = unordered_map<column_t, LogicalType>;
using GetConflicts = unordered_set<column_t>;
using GetReplace = unordered_map<column_t, LogicalType>;

struct GetAnalysis {
	LogicalGet *get;
	GetCastMap cast_map;
	GetConflicts conflicts;
};

using Analyses = unordered_map<TableIndex, GetAnalysis>;
using Replace = unordered_map<TableIndex, GetReplace>;

// Collect expressions of form CAST(bound column, T) -> LOGICAL_GET.
// If bound column is already cast to a different type or used uncasted, record
// in "conflicts".
static void CollectCastTypes(const Expression &expr, Analyses &analyses) {
	auto collect_children = [&] {
		ExpressionIterator::EnumerateChildren(expr,
		                                      [&](const Expression &child) { CollectCastTypes(child, analyses); });
	};

	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		const auto &colref = expr.Cast<BoundColumnRefExpression>();
		if (colref.depth != 0) {
			return;
		}
		const auto it = analyses.find(colref.binding.table_index);
		if (it == analyses.end()) {
			return;
		}
		GetAnalysis &analysis = it->second;
		const column_t proj_id = colref.binding.column_index;
		if (!IsVirtualColumn(proj_id)) {
			const column_t column_id = analysis.get->GetColumnIds()[proj_id].GetPrimaryIndex();
			// Column is used uncasted
			analysis.conflicts.insert(column_id);
		}
		return;
	}

	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CAST) {
		return collect_children();
	}
	const auto &bound_cast = expr.Cast<BoundCastExpression>();

	if (bound_cast.child->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return collect_children();
	}
	const auto &bound_column = bound_cast.child->Cast<BoundColumnRefExpression>();

	if (bound_column.depth > 0) {
		return collect_children();
	}

	const auto it = analyses.find(bound_column.binding.table_index);
	if (it == analyses.end()) {
		return collect_children();
	}
	// We're in a leaf

	const column_t projection_id = bound_column.binding.column_index;
	if (IsVirtualColumn(projection_id)) {
		return;
	}

	GetAnalysis &analysis = it->second;
	GetCastMap &cast_map = analysis.cast_map;
	const column_t column_id = analysis.get->GetColumnIds()[projection_id].GetPrimaryIndex();
	if (auto cast_it = cast_map.find(column_id); cast_it == cast_map.end()) {
		cast_map.emplace(column_id, bound_cast.GetReturnType());
	} else if (cast_it->second != bound_cast.GetReturnType()) {
		analysis.conflicts.insert(column_id);
	}
}

// Replace CAST(BoundColumn, T) where BoundColumn is a leaf with a ColumnRef(T)
static void ReplaceCastTypes(unique_ptr<Expression> &expr, const Replace &replace) {
	auto replace_children = [&] {
		ExpressionIterator::EnumerateChildren(*expr,
		                                      [&](unique_ptr<Expression> &child) { ReplaceCastTypes(child, replace); });
	};

	if (expr->GetExpressionClass() != ExpressionClass::BOUND_CAST) {
		return replace_children();
	}
	const auto &bound_cast = expr->Cast<BoundCastExpression>();

	if (bound_cast.child->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return replace_children();
	}
	const auto &bound_column = bound_cast.child->Cast<BoundColumnRefExpression>();

	if (bound_column.depth > 0) {
		return replace_children();
	}

	const auto replace_it = replace.find(bound_column.binding.table_index);
	if (replace_it == replace.end()) {
		return replace_children();
	}

	const column_t projection_id = bound_column.binding.column_index;
	const auto &get_replace = replace_it->second;
	const auto get_replace_it = get_replace.find(projection_id);

	if (get_replace_it == get_replace.end() || get_replace_it->second != bound_cast.GetReturnType()) {
		return replace_children();
	}

	expr = make_uniq<BoundColumnRefExpression>(get_replace_it->second, bound_column.binding);
}

static void CollectFromOp(LogicalOperator &op, Analyses &analyses) {
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](auto *expr_ptr) { CollectCastTypes(**expr_ptr, analyses); });
	for (auto &child : op.children) {
		CollectFromOp(*child, analyses);
	}
}

static void ReplaceInOp(LogicalOperator &op, const Replace &replacements) {
	LogicalOperatorVisitor::EnumerateExpressions(op,
	                                             [&](auto *expr_ptr) { ReplaceCastTypes(*expr_ptr, replacements); });
	for (auto &child : op.children) {
		ReplaceInOp(*child, replacements);
	}
}

static void FindGetsWithTypePushdown(LogicalOperator &op, vector<LogicalGet *> &gets) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		if (auto &get = op.Cast<LogicalGet>(); get.function.type_pushdown != nullptr) {
			gets.push_back(&get);
		}
	}
	for (auto &child : op.children) {
		FindGetsWithTypePushdown(*child, gets);
	}
}

unique_ptr<LogicalOperator> TypePushdown::Optimize(unique_ptr<LogicalOperator> op) {
	vector<LogicalGet *> gets;
	FindGetsWithTypePushdown(*op, gets);
	if (gets.empty()) {
		return op;
	}

	Analyses analyses(gets.size());
	for (size_t i = 0; i < gets.size(); ++i) {
		analyses.emplace(gets[i]->table_index.index, GetAnalysis {gets[i]});
	}
	CollectFromOp(*op, analyses);

	Replace replace;
	for (auto &[table_index, analysis] : analyses) {
		for (column_t col_id : analysis.conflicts) {
			analysis.cast_map.erase(col_id);
		}
		if (analysis.cast_map.empty()) {
			continue;
		}

		LogicalGet &get = *analysis.get;
		get.function.type_pushdown(context, get.bind_data, analysis.cast_map);
		for (const auto &[col_id, new_type] : analysis.cast_map) {
			get.returned_types[col_id] = new_type;
		}

		const vector<ColumnIndex> &column_ids = get.GetColumnIds();
		GetReplace &get_replace = replace[table_index];
		const GetCastMap &cast_map = analysis.cast_map;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			const column_t col_idx = column_ids[i].GetPrimaryIndex();
			if (const auto it = cast_map.find(col_idx); it != cast_map.end()) {
				get_replace[i] = it->second;
			}
		}
	}

	if (!replace.empty()) {
		ReplaceInOp(*op, replace);
	}
	return op;
}
} // namespace duckdb
