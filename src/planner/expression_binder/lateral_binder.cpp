#include "duckdb/planner/expression_binder/lateral_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"

namespace duckdb {

LateralBinder::LateralBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

void LateralBinder::ExtractCorrelatedColumns(Expression &expr) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
		if (bound_colref.depth > 0) {
			// add the correlated column info
			CorrelatedColumnInfo info(bound_colref);
			if (std::find(correlated_columns.begin(), correlated_columns.end(), info) == correlated_columns.end()) {
				correlated_columns.push_back(std::move(info));
			}
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { ExtractCorrelatedColumns(child); });
}

BindResult LateralBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	if (depth == 0) {
		throw InternalException("Lateral binder can only bind correlated columns");
	}
	auto result = ExpressionBinder::BindExpression(expr_ptr, depth);
	if (result.HasError()) {
		return result;
	}
	ExtractCorrelatedColumns(*result.expression);
	return result;
}

BindResult LateralBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult("LATERAL join cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("LATERAL join cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth, root_expression);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string LateralBinder::UnsupportedAggregateMessage() {
	return "LATERAL join cannot contain aggregates!";
}

class ExpressionDepthReducer : public LogicalOperatorVisitor {
public:
	explicit ExpressionDepthReducer(const vector<CorrelatedColumnInfo> &correlated) : correlated_columns(correlated) {
	}

protected:
	void ReduceColumnRefDepth(BoundColumnRefExpression &expr) {
		// don't need to reduce this
		if (expr.depth == 0) {
			return;
		}
		for (auto &correlated : correlated_columns) {
			if (correlated.binding == expr.binding) {
				D_ASSERT(expr.depth > 1);
				expr.depth--;
				break;
			}
		}
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		ReduceColumnRefDepth(expr);
		return nullptr;
	}

	void ReduceExpressionSubquery(BoundSubqueryExpression &expr) {
		for (auto &s_correlated : expr.binder->correlated_columns) {
			for (auto &correlated : correlated_columns) {
				if (correlated == s_correlated) {
					s_correlated.depth--;
					break;
				}
			}
		}
	}

	void ReduceExpressionDepth(Expression &expr) {
		if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			ReduceColumnRefDepth(expr.Cast<BoundColumnRefExpression>());
		}
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
			auto &subquery_ref = expr.Cast<BoundSubqueryExpression>();
			ReduceExpressionSubquery(expr.Cast<BoundSubqueryExpression>());
			// Recursively update the depth in the bindings of the children nodes
			ExpressionIterator::EnumerateQueryNodeChildren(
			    *subquery_ref.subquery, [&](Expression &child_expr) { ReduceExpressionDepth(child_expr); });
		}
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		ReduceExpressionSubquery(expr);
		ExpressionIterator::EnumerateQueryNodeChildren(
		    *expr.subquery, [&](Expression &child_expr) { ReduceExpressionDepth(child_expr); });
		return nullptr;
	}

	const vector<CorrelatedColumnInfo> &correlated_columns;
};

void LateralBinder::ReduceExpressionDepth(LogicalOperator &op, const vector<CorrelatedColumnInfo> &correlated) {
	ExpressionDepthReducer depth_reducer(correlated);
	depth_reducer.VisitOperator(op);
}

} // namespace duckdb
