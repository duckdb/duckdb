#include "duckdb/optimizer/type_pushdown.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

/*
 * This pass implements type pushdown for file readers. If a LOGICAL_PROJECTION
 * has a LOGICAL_GET as a child, and for some column that's cast in
 * LOGICAL_PROJECTION there is no other usage (including uncasted) in the query,
 * and file reader supports projection expression pushdown, we can push down the
 * type cast into LOGICAL_GET.
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

// A passthrough projection only forwards its child columns, e.g. a VIEW's
// "SELECT col".
static bool IsPassthrough(const LogicalProjection &projection) {
	if (projection.expressions.empty()) {
		return false; // don't register empty projections in Projections
	}
	for (const auto &e : projection.expressions) {
		if (e->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
	}
	return true;
}

void FindGetsAndProjections(LogicalOperator &op, Analyses &analyses, Projections &projections) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		if (auto &get = op.Cast<LogicalGet>(); get.function.projection_expression_pushdown != nullptr) {
			analyses.emplace(get.table_index, GetAnalysis {get, {}});
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		LogicalProjection &projection = op.Cast<LogicalProjection>();
		D_ASSERT(projection.children.size() == 1);
		auto &child = *projection.children[0];
		if (!IsPassthrough(projection) || child.type != LogicalOperatorType::LOGICAL_GET) {
			break;
		}
		if (auto &get = child.Cast<LogicalGet>(); get.function.projection_expression_pushdown != nullptr) {
			projections.emplace(projection.table_index, projection);
		}
		break;
	}
	default:
		break;
	}

	for (auto &child : op.children) {
		FindGetsAndProjections(*child, analyses, projections);
	}
}

std::optional<GetBinding> Resolve(ColumnBinding binding, Analyses &analyses, const Projections &projections) {
	if (IsVirtualColumn(binding.column_index)) {
		return std::nullopt;
	}
	if (const auto it = analyses.find(binding.table_index); it != analyses.end()) {
		return {{it->second, binding.column_index, nullptr}};
	}

	const auto projection_it = projections.find(binding.table_index);
	if (projection_it == projections.end()) {
		return std::nullopt;
	}

	LogicalProjection &projection = projection_it->second;
	const auto &inner = projection.expressions[binding.column_index];
	if (inner->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return std::nullopt;
	}
	const ColumnBinding get_binding = inner->Cast<BoundColumnRefExpression>().Binding();
	if (IsVirtualColumn(get_binding.column_index)) {
		return std::nullopt;
	}
	if (const auto it = analyses.find(get_binding.table_index); it != analyses.end()) {
		return {{it->second, get_binding.column_index, &projection}};
	}
	return std::nullopt;
}

// A GET reachable through a single-child chain of filters/projections. A join
// (or any other multi-child operator) breaks the chain.
// See test/sql/copy/csv/test_insert_into_types.test (cast not pushed past a join)
static bool ReachesPushdownGet(const LogicalOperator &op) {
	const LogicalOperator *cur = &op;
	while (cur->children.size() == 1) {
		cur = cur->children[0].get();
		switch (cur->type) {
		case LogicalOperatorType::LOGICAL_GET:
			return cur->Cast<LogicalGet>().function.projection_expression_pushdown != nullptr;
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_PROJECTION:
			continue;
		default:
			return false;
		}
	}
	return false;
}

void CastCollect::VisitOperator(LogicalOperator &op) {
	/*
	 * Logical projection expressions are columns which reference underlying
	 * GETs. Don't process them, as they would add conflicts for every column
	 * used in projection. Example: PROJECTION(col) -> GET(col). We don't want
	 * to visit BoundColumnRefExpression in PROJECTION to avoid registering a
	 * non-existent conflict.
	 *
	 * However, CastReplace will visit them because we need to update their
	 * types if pushdown succeeded.
	 */
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return LogicalOperatorVisitor::VisitOperator(op);
	}
	auto &projection = op.Cast<LogicalProjection>();

	// Only push casts from a projection that forwards just column refs and
	// casts and reaches a GET without a join in between. A constant or other
	// expression makes the projection ineligible.
	// See test/sql/copy/csv/test_csv_error_message_type.test (top-level cast
	// to VARCHAR must still push) and test_large_integer_detection.test (a
	// nested cast to VARCHAR must not).
	bool clean = ReachesPushdownGet(projection);
	for (const auto &e : projection.expressions) {
		switch (e->GetExpressionClass()) {
		case ExpressionClass::BOUND_COLUMN_REF:
		case ExpressionClass::BOUND_CAST:
			continue;
		default:
			clean = false;
			break;
		}
	}
	if (clean) {
		for (const auto &e : projection.expressions) {
			if (e->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
				top_level_casts.insert(e.get());
			}
		}
	}
	if (projections.count(projection.table_index)) {
		VisitOperatorChildren(op);
		return;
	}

	LogicalOperatorVisitor::VisitOperator(op);
}

unique_ptr<Expression> CastCollect::VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *ptr) {
	if (const auto binding = Resolve(expr.Binding(), analyses, projections)) {
		// Column is used without cast applied to it, register a conflict.
		// Not emplace() as we need to update the value if it was present
		binding->analysis.col_to_cast[binding->column_index] = nullptr;
	}
	return std::move(*ptr);
}

unique_ptr<Expression> CastCollect::VisitReplace(BoundCastExpression &expr, unique_ptr<Expression> *ptr) {
	if (expr.Child().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		// Descend into children so e.g. fn(col, other) still sees "col" and
		// registers a conflict
		return nullptr;
	}
	const auto &bound_col = expr.Child().Cast<BoundColumnRefExpression>();
	const auto binding = Resolve(bound_col.Binding(), analyses, projections);
	if (!binding) {
		return nullptr;
	}
	auto &col_to_cast = binding->analysis.col_to_cast;

	if (auto it = col_to_cast.find(binding->column_index); it == col_to_cast.end()) {
		// Only a top-level projection cast starts a candidate.
		if (top_level_casts.count(&expr)) {
			col_to_cast.emplace(binding->column_index, &expr);
		}
	} else if (it->second == nullptr || it->second->GetReturnType() != expr.GetReturnType()) {
		// Different target type, or already a conflict.
		// TODO(myrrc): CAST and TRY_CAST to the same type don't conflict
		// but what if reader can push down TRY_CAST but not CAST?
		it->second = nullptr;
	}

	return std::move(*ptr);
}

static bool can_pushdown_column(const GetAnalysis &analysis, ProjectionIndex idx) {
	const auto it = analysis.col_to_cast.find(idx);
	return it != analysis.col_to_cast.end() && it->second != nullptr;
}

unique_ptr<Expression> CastReplace::VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *ptr) {
	const auto binding = Resolve(expr.Binding(), analyses, projections);
	if (!binding) {
		return std::move(*ptr);
	}

	const auto &[analysis, column_index, projection] = *binding;
	if (can_pushdown_column(analysis, column_index)) {
		const idx_t storage_index = analysis.get.GetColumnIds()[column_index].GetPrimaryIndex();
		const LogicalType return_type = analysis.get.returned_types[storage_index];
		expr.SetReturnType(return_type);
		// LogicalProjection types are resolved by calling
		// LogicalProjection::ResolveTypes, so we need to check whether types in
		// projection have been resolved, and updated them only if needed.
		if (projection != nullptr && !projection->types.empty()) {
			projection->types[column_index] = return_type;
		}
	}

	return std::move(*ptr);
}

unique_ptr<Expression> CastReplace::VisitReplace(BoundCastExpression &expr, unique_ptr<Expression> *ptr) {
	if (expr.Child().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return nullptr; // Same as in ScalarFnCollect::VisitReplace
	}
	auto &bound_col_base = expr.ChildMutable();
	const auto &bound_col = bound_col_base->Cast<BoundColumnRefExpression>();
	const auto binding = Resolve(bound_col.Binding(), analyses, projections);
	if (!binding) {
		return nullptr;
	}

	const auto &[analysis, column_index, projection] = *binding;
	if (!can_pushdown_column(analysis, column_index)) {
		return std::move(*ptr);
	}

	const idx_t storage_index = analysis.get.GetColumnIds()[column_index].GetPrimaryIndex();
	const LogicalType return_type = analysis.get.returned_types[storage_index];
	bound_col_base->SetReturnType(return_type);
	// Same as in CastReplace::VisitReplace(BoundColumnRefExpression)
	if (projection != nullptr && !projection->types.empty()) {
		projection->types[column_index] = return_type;
	}
	return std::move(bound_col_base);
}

CastCollect::CastCollect(Analyses &analyses, const Projections &projections)
    : analyses(analyses), projections(projections) {
}

CastReplace::CastReplace(Analyses &analyses, const Projections &projections)
    : analyses(analyses), projections(projections) {
}

unique_ptr<LogicalOperator> TypePushdown::Optimize(unique_ptr<LogicalOperator> op) {
	Analyses analyses;
	Projections projections;
	FindGetsAndProjections(*op, analyses, projections);
	if (analyses.empty()) {
		return op;
	}
	CastCollect(analyses, projections).VisitOperator(*op);

	bool any_pushed = false;
	for (auto &[_, analysis] : analyses) {
		for (auto &[column_index, expr] : analysis.col_to_cast) {
			if (expr == nullptr) { // Conflict for column
				continue;
			}
			const idx_t storage_index = analysis.get.GetColumnIds()[column_index].GetPrimaryIndex();
			TableFunctionProjectionExpressionInput input {analysis.get, *expr, storage_index};
			if (analysis.get.function.projection_expression_pushdown(context, input)) {
				// LOGICAL_GET doesn't initialize .types of LogicalOperator
				analysis.get.returned_types[storage_index] = expr->GetReturnType();
				any_pushed = true;
			} else { // failed to push down expression, can't replace it
				expr = nullptr;
			}
		}
	}

	if (any_pushed) {
		CastReplace(analyses, projections).VisitOperator(*op);
	}
	return op;
}
} // namespace duckdb
