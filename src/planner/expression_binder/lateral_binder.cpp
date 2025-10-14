#include "duckdb/planner/expression_binder/lateral_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

LateralBinder::LateralBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

void LateralBinder::ExtractCorrelatedColumns(Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
		if (bound_colref.depth > 0) {
			// add the correlated column info
			CorrelatedColumnInfo info(bound_colref);
			if (std::find(correlated_columns.begin(), correlated_columns.end(), info) == correlated_columns.end()) {
				correlated_columns.AddColumn(std::move(info)); // TODO is adding to the front OK here?
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
		return BindUnsupportedExpression(expr, depth, "LATERAL join cannot contain DEFAULT clause!");
	case ExpressionClass::WINDOW:
		return BindUnsupportedExpression(expr, depth, "LATERAL join cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth, root_expression);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string LateralBinder::UnsupportedAggregateMessage() {
	return "LATERAL join cannot contain aggregates!";
}

static void ReduceColumnRefDepth(BoundColumnRefExpression &expr, const CorrelatedColumns &correlated_columns) {
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

static void ReduceColumnDepth(CorrelatedColumns &columns, const CorrelatedColumns &affected_columns) {
	for (auto &s_correlated : columns) {
		for (auto &affected : affected_columns) {
			if (affected == s_correlated) {
				s_correlated.depth--;
				break;
			}
		}
	}
}

class ExpressionDepthReducerRecursive : public LogicalOperatorVisitor {
public:
	explicit ExpressionDepthReducerRecursive(const CorrelatedColumns &correlated) : correlated_columns(correlated) {
	}

	void VisitExpression(unique_ptr<Expression> *expression) override {
		auto &expr = **expression;
		if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			ReduceColumnRefDepth(expr.Cast<BoundColumnRefExpression>(), correlated_columns);
		} else if (expr.GetExpressionType() == ExpressionType::SUBQUERY) {
			ReduceExpressionSubquery(expr.Cast<BoundSubqueryExpression>(), correlated_columns);
		}
		LogicalOperatorVisitor::VisitExpression(expression);
	}

	void VisitOperator(LogicalOperator &op) override {
		if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
			// rewrite correlated columns in child joins
			auto &bound_join = op.Cast<LogicalDependentJoin>();
			ReduceColumnDepth(bound_join.correlated_columns, correlated_columns);
		}
		// visit the children of the table ref
		LogicalOperatorVisitor::VisitOperator(op);
	}

	static void ReduceExpressionSubquery(BoundSubqueryExpression &expr, const CorrelatedColumns &correlated_columns) {
		ReduceColumnDepth(expr.binder->correlated_columns, correlated_columns);
		ExpressionDepthReducerRecursive recursive(correlated_columns);
		recursive.VisitOperator(*expr.subquery.plan);
	}

private:
	const CorrelatedColumns &correlated_columns;
};

class ExpressionDepthReducer : public LogicalOperatorVisitor {
public:
	explicit ExpressionDepthReducer(const CorrelatedColumns &correlated) : correlated_columns(correlated) {
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		ReduceColumnRefDepth(expr, correlated_columns);
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		ExpressionDepthReducerRecursive::ReduceExpressionSubquery(expr, correlated_columns);
		return nullptr;
	}

	const CorrelatedColumns &correlated_columns;
};

void LateralBinder::ReduceExpressionDepth(LogicalOperator &op, const CorrelatedColumns &correlated) {
	ExpressionDepthReducer depth_reducer(correlated);
	depth_reducer.VisitOperator(op);
}

} // namespace duckdb
