#include "duckdb/planner/expression_binder/lateral_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

LateralBinder::LateralBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

void LateralBinder::ExtractCorrelatedColumns(Expression &expr) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = (BoundColumnRefExpression &)expr;
		if (bound_colref.depth > 0) {
			// add the correlated column info
			CorrelatedColumnInfo info(bound_colref);
			if (std::find(correlated_columns.begin(), correlated_columns.end(), info) == correlated_columns.end()) {
				correlated_columns.push_back(move(info));
			}
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { ExtractCorrelatedColumns(child); });
}

BindResult LateralBinder::BindColumnRef(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	if (depth == 0) {
		throw InternalException("Lateral binder can only bind correlated columns");
	}
	auto result = ExpressionBinder::BindExpression(expr_ptr, depth);
	if (result.HasError()) {
		return result;
	}
	if (depth > 1) {
		throw BinderException("Nested lateral joins are not supported yet");
	}
	ExtractCorrelatedColumns(*result.expression);
	return result;
}

vector<CorrelatedColumnInfo> LateralBinder::ExtractCorrelatedColumns(Binder &binder) {
	auto all_correlated_columns = binder.correlated_columns;
	for (auto &correlated : correlated_columns) {
		auto entry = std::find(binder.correlated_columns.begin(), binder.correlated_columns.end(), correlated);
		if (entry == binder.correlated_columns.end()) {
			throw InternalException("Lateral Binder: could not find correlated column in binder");
		}
		binder.correlated_columns.erase(entry);
	}
	return all_correlated_columns;
}

BindResult LateralBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
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
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		// don't need to reduce this
		if (expr.depth == 0) {
			return nullptr;
		}
		for (auto &correlated : correlated_columns) {
			if (correlated.binding == expr.binding) {
				D_ASSERT(expr.depth > 1);
				expr.depth--;
				break;
			}
		}
		return nullptr;
	}

	const vector<CorrelatedColumnInfo> &correlated_columns;
};

void LateralBinder::ReduceExpressionDepth(LogicalOperator &op, const vector<CorrelatedColumnInfo> &correlated) {
	ExpressionDepthReducer depth_reducer(correlated);
	depth_reducer.VisitOperator(op);
}

} // namespace duckdb
