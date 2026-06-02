#include "duckdb/optimizer/type_pushdown.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

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

using Gets = vector<reference<LogicalGet>>;
using GetCastMap = unordered_map<column_t, LogicalType>;
using GetConflicts = unordered_set<ProjectionIndex>;
using GetReplace = unordered_map<ProjectionIndex, LogicalType>;

struct GetAnalysis {
	reference<LogicalGet> get;
	GetCastMap cast_map;
	GetConflicts conflicts;
};

using Analyses = unordered_map<TableIndex, GetAnalysis>;
using Replace = unordered_map<TableIndex, GetReplace>;

// Collect expressions of form CAST(bound column, T) -> LOGICAL_GET.
// If bound column is already cast to a different type or used uncasted, record
// in "conflicts".
struct CastCollectVisitor final : LogicalOperatorVisitor {
	Analyses &analyses;
	explicit CastCollectVisitor(Analyses &analyses) : analyses(analyses) {
	}
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundCastExpression &expr, unique_ptr<Expression> *expr_ptr) override;
};

unique_ptr<Expression> CastCollectVisitor::VisitReplace(BoundColumnRefExpression &expr,
                                                        unique_ptr<Expression> *expr_ptr) {
	const auto it = analyses.find(expr.Binding().table_index);
	if (it == analyses.end()) {
		return std::move(*expr_ptr);
	}
	GetAnalysis &analysis = it->second;
	const column_t proj_id = expr.Binding().column_index;
	if (!IsVirtualColumn(proj_id)) {
		const ProjectionIndex index {analysis.get.get().GetColumnIds()[proj_id].GetPrimaryIndex()};
		// Column is used uncasted
		analysis.conflicts.insert(index);
	}
	return std::move(*expr_ptr);
}

unique_ptr<Expression> CastCollectVisitor::VisitReplace(BoundCastExpression &expr, unique_ptr<Expression> *expr_ptr) {
	if (expr.Child().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return std::move(*expr_ptr);
	}
	const auto &bound_column = expr.Child().Cast<BoundColumnRefExpression>();
	const auto it = analyses.find(bound_column.Binding().table_index);
	if (it == analyses.end()) {
		return std::move(*expr_ptr);
	}

	// We're in a leaf
	const column_t projection_id = bound_column.Binding().column_index;
	if (IsVirtualColumn(projection_id)) {
		return std::move(*expr_ptr);
	}

	GetAnalysis &analysis = it->second;
	GetCastMap &cast_map = analysis.cast_map;
	const ColumnIndex &column_index = analysis.get.get().GetColumnIds()[projection_id];
	if (column_index.IsPushdownExtract()) {
		throw InternalException("PUSHDOWN_EXTRACT column in index");
	}
	const ProjectionIndex proj_idx {column_index.GetPrimaryIndex()};

	if (auto cast_it = cast_map.find(proj_idx); cast_it == cast_map.end()) {
		cast_map.emplace(proj_idx, expr.GetReturnType());
	} else if (cast_it->second != expr.GetReturnType()) {
		analysis.conflicts.insert(proj_idx);
	}

	return std::move(*expr_ptr);
}

// Replace CAST(BoundColumn, T) where BoundColumn is a leaf with a ColumnRef(T)
struct CastReplaceVisitor final : LogicalOperatorVisitor {
	const Replace &replace;
	explicit CastReplaceVisitor(const Replace &replace) : replace(replace) {
	}

	unique_ptr<Expression> VisitReplace(BoundCastExpression &expr, unique_ptr<Expression> *expr_ptr) override;
};

unique_ptr<Expression> CastReplaceVisitor::VisitReplace(BoundCastExpression &expr, unique_ptr<Expression> *expr_ptr) {
	if (expr.Child().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return std::move(*expr_ptr);
	}

	const auto &bound_column = expr.Child().Cast<BoundColumnRefExpression>();
	if (bound_column.Depth() > 0) {
		throw InternalException("BoundColumnRef with non-zero depth");
	}

	const auto replace_it = replace.find(bound_column.Binding().table_index);
	if (replace_it == replace.end()) {
		return std::move(*expr_ptr);
	}

	const ProjectionIndex projection_id = bound_column.Binding().column_index;
	const auto &get_replace = replace_it->second;
	const auto get_replace_it = get_replace.find(projection_id);

	if (get_replace_it == get_replace.end() || get_replace_it->second != expr.GetReturnType()) {
		return std::move(*expr_ptr);
	}

	return make_uniq<BoundColumnRefExpression>(get_replace_it->second, bound_column.Binding());
}

static void FindGetsWithTypePushdown(LogicalOperator &op, Gets &gets) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (get.table_filters.FilterCount() > 0) {
			throw InternalException("TypePushdown optimizer run after FilterPushdown");
		}

		if (get.function.type_pushdown != nullptr) {
			gets.emplace_back(get);
		}
	}
	for (auto &child : op.children) {
		FindGetsWithTypePushdown(*child, gets);
	}
}

unique_ptr<LogicalOperator> TypePushdown::Optimize(unique_ptr<LogicalOperator> op) {
	Gets gets;
	FindGetsWithTypePushdown(*op, gets);
	if (gets.empty()) {
		return op;
	}

	Analyses analyses(gets.size());
	for (idx_t i = 0; i < gets.size(); ++i) {
		analyses.emplace(gets[i].get().table_index, GetAnalysis {gets[i]});
	}
	CastCollectVisitor {analyses}.VisitOperator(op);

	Replace replace;
	for (auto &[table_index, analysis] : analyses) {
		for (ProjectionIndex idx : analysis.conflicts) {
			analysis.cast_map.erase(idx);
		}
		if (analysis.cast_map.empty()) {
			continue;
		}

		LogicalGet &get = analysis.get.get();
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
				get_replace[ProjectionIndex {i}] = it->second;
			}
		}
	}

	if (!replace.empty()) {
		CastReplaceVisitor {replace}.VisitOperator(op);
	}
	return op;
}
} // namespace duckdb
