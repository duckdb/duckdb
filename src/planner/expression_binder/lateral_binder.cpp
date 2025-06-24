#include "duckdb/planner/expression_binder/lateral_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"

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

static void ReduceColumnRefDepth(BoundColumnRefExpression &expr,
                                 const vector<CorrelatedColumnInfo> &correlated_columns) {
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

static void ReduceColumnDepth(vector<CorrelatedColumnInfo> &columns,
                              const vector<CorrelatedColumnInfo> &affected_columns) {
	for (auto &s_correlated : columns) {
		for (auto &affected : affected_columns) {
			if (affected == s_correlated) {
				s_correlated.depth--;
				break;
			}
		}
	}
}

class ExpressionDepthReducerRecursive : public BoundNodeVisitor {
public:
	explicit ExpressionDepthReducerRecursive(const vector<CorrelatedColumnInfo> &correlated)
	    : correlated_columns(correlated) {
	}

	void VisitExpression(unique_ptr<Expression> &expression) override {
		if (expression->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			ReduceColumnRefDepth(expression->Cast<BoundColumnRefExpression>(), correlated_columns);
		} else if (expression->GetExpressionType() == ExpressionType::SUBQUERY) {
			ReduceExpressionSubquery(expression->Cast<BoundSubqueryExpression>(), correlated_columns);
		}
		BoundNodeVisitor::VisitExpression(expression);
	}

	void VisitBoundTableRef(BoundTableRef &ref) override {
		if (ref.type == TableReferenceType::JOIN) {
			// rewrite correlated columns in child joins
			auto &bound_join = ref.Cast<BoundJoinRef>();
			ReduceColumnDepth(bound_join.correlated_columns, correlated_columns);
		}
		// visit the children of the table ref
		BoundNodeVisitor::VisitBoundTableRef(ref);
	}

	static void ReduceExpressionSubquery(BoundSubqueryExpression &expr,
	                                     const vector<CorrelatedColumnInfo> &correlated_columns) {
		ReduceColumnDepth(expr.binder->correlated_columns, correlated_columns);
		ExpressionDepthReducerRecursive recursive(correlated_columns);
		recursive.VisitBoundQueryNode(*expr.subquery);
	}

private:
	const vector<CorrelatedColumnInfo> &correlated_columns;
};

class ExpressionDepthReducer : public LogicalOperatorVisitor {
public:
	explicit ExpressionDepthReducer(const vector<CorrelatedColumnInfo> &correlated) : correlated_columns(correlated) {
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

	const vector<CorrelatedColumnInfo> &correlated_columns;
};

void LateralBinder::ReduceExpressionDepth(LogicalOperator &op, const vector<CorrelatedColumnInfo> &correlated) {
	ExpressionDepthReducer depth_reducer(correlated);
	depth_reducer.VisitOperator(op);
}

} // namespace duckdb
