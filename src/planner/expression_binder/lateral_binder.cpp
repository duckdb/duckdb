#include "duckdb/planner/expression_binder/lateral_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

LateralBinder::LateralBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

void LateralBinder::ExtractCorrelatedColumns(Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
		if (bound_colref.Depth() > 0) {
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

static void ReduceColumnRefDepth(BoundColumnRefExpression &expr, const CorrelatedColumns &correlated_columns) {
	// don't need to reduce this
	if (expr.Depth() == 0) {
		return;
	}
	for (auto &correlated : correlated_columns) {
		if (correlated.binding == expr.Binding()) {
			D_ASSERT(expr.Depth() > 1);
			expr.DepthMutable()--;
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
		ReduceColumnDepth(expr.GetBinder()->correlated_columns, correlated_columns);
		if (expr.SubqueryMutable().plan) {
			ExpressionDepthReducerRecursive recursive(correlated_columns);
			recursive.VisitOperator(*expr.SubqueryMutable().plan);
		}
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

static void AddLateralCorrelation(CorrelatedColumns &correlated_columns, CorrelatedColumnInfo info) {
	for (auto &existing : correlated_columns) {
		if (existing == info) {
			return;
		}
	}
	correlated_columns.AddColumn(std::move(info));
}

static void AddLateralCorrelation(CorrelatedColumns &correlated_columns, const BoundColumnRefExpression &colref) {
	AddLateralCorrelation(correlated_columns, CorrelatedColumnInfo(colref.Binding(), colref.GetReturnType(),
	                                                               colref.GetName(), colref.Depth() + 1));
}

static bool IsBindingIn(const ColumnBinding &binding, const unordered_set<TableIndex> &bindings) {
	return bindings.find(binding.table_index) != bindings.end();
}

static bool ReferenceEscapesSubqueryScope(idx_t reference_depth, idx_t scope_depth) {
	if (scope_depth == 0) {
		return reference_depth > 0;
	}
	return reference_depth > scope_depth;
}

static void AddDepthIncreasedCorrelation(CorrelatedColumns &correlations, const CorrelatedColumnInfo &column) {
	auto copy = column;
	copy.depth++;
	AddLateralCorrelation(correlations, std::move(copy));
}

class ExternalExpressionDepthIncreaser : public LogicalOperatorVisitor {
public:
	explicit ExternalExpressionDepthIncreaser(CorrelatedColumns &increased_columns)
	    : increased_columns(increased_columns) {
	}

	void Increase(LogicalOperator &op) {
		CollectLocalBindings(op);
		VisitOperator(op);
	}

protected:
	void VisitOperator(LogicalOperator &op) override {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
			IncreaseCorrelatedColumns(op.Cast<LogicalDependentJoin>().correlated_columns);
			break;
		case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			IncreaseCorrelatedColumns(op.Cast<LogicalCTE>().correlated_columns);
			break;
		default:
			break;
		}
		LogicalOperatorVisitor::VisitOperator(op);
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.Depth() > 0 && !IsBindingIn(expr.Binding(), local_bindings)) {
			CorrelatedColumnInfo correlation(expr);
			expr.DepthMutable()++;
			AddDepthIncreasedCorrelation(increased_columns, correlation);
		}
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		IncreaseCorrelatedColumns(expr.GetBinder()->correlated_columns);
		if (expr.SubqueryMutable().plan) {
			CollectLocalBindings(*expr.SubqueryMutable().plan);
			VisitOperator(*expr.SubqueryMutable().plan);
		}
		return nullptr;
	}

private:
	void CollectLocalBindings(LogicalOperator &op) {
		auto bindings = op.GetColumnBindings();
		for (auto &binding : bindings) {
			local_bindings.insert(binding.table_index);
		}
		for (auto &child : op.children) {
			CollectLocalBindings(*child);
		}
	}

	void IncreaseCorrelatedColumns(CorrelatedColumns &columns) {
		for (auto &column : columns) {
			if (column.depth > 0 && !IsBindingIn(column.binding, local_bindings)) {
				auto old_column = column;
				column.depth++;
				AddDepthIncreasedCorrelation(increased_columns, old_column);
			}
		}
	}

	unordered_set<TableIndex> local_bindings;
	CorrelatedColumns &increased_columns;
};

class PairDependentJoinConditionCorrelator : public LogicalOperatorVisitor {
public:
	PairDependentJoinConditionCorrelator(const unordered_set<TableIndex> &left_bindings,
	                                     const unordered_set<TableIndex> &right_bindings,
	                                     CorrelatedColumns &correlated_columns)
	    : left_bindings(left_bindings), right_bindings(right_bindings), correlated_columns(correlated_columns) {
	}

protected:
	void VisitOperator(LogicalOperator &op) override {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
			LateralizeCorrelatedColumns(op.Cast<LogicalDependentJoin>().correlated_columns, subquery_plan_depth + 1);
			subquery_plan_depth++;
			LogicalOperatorVisitor::VisitOperator(op);
			subquery_plan_depth--;
			return;
		}
		case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			LateralizeCorrelatedColumns(op.Cast<LogicalCTE>().correlated_columns, subquery_plan_depth);
			break;
		default:
			break;
		}
		LogicalOperatorVisitor::VisitOperator(op);
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (NeedsLateralRewrite(expr.Binding(), expr.Depth(), subquery_plan_depth)) {
			AddLateralCorrelation(correlated_columns, expr);
			expr.DepthMutable()++;
		}
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		for (auto &corr : expr.GetBinder()->correlated_columns) {
			if (NeedsLateralRewrite(corr.binding, corr.depth, subquery_plan_depth + 1)) {
				AddLateralCorrelation(correlated_columns, corr);
				corr.depth++;
			}
		}
		if (expr.SubqueryMutable().plan) {
			subquery_plan_depth++;
			VisitOperator(*expr.SubqueryMutable().plan);
			subquery_plan_depth--;
		}
		return nullptr;
	}

private:
	void LateralizeCorrelatedColumns(CorrelatedColumns &columns, idx_t scope_depth) {
		for (auto &corr : columns) {
			if (NeedsLateralRewrite(corr.binding, corr.depth, scope_depth)) {
				AddLateralCorrelation(correlated_columns, corr);
				corr.depth++;
			}
		}
	}

	bool NeedsLateralRewrite(const ColumnBinding &binding, idx_t reference_depth, idx_t join_scope_depth) const {
		if (IsBindingIn(binding, right_bindings)) {
			return false;
		}
		if (IsBindingIn(binding, left_bindings)) {
			return true;
		}
		return ReferenceEscapesSubqueryScope(reference_depth, join_scope_depth);
	}

	const unordered_set<TableIndex> &left_bindings;
	const unordered_set<TableIndex> &right_bindings;
	CorrelatedColumns &correlated_columns;
	idx_t subquery_plan_depth = 0;
};

bool LateralBinder::ExtractPairDependentJoinConditionCorrelations(LogicalOperator &lateral_child,
                                                                  unique_ptr<Expression> &condition,
                                                                  const unordered_set<TableIndex> &left_bindings,
                                                                  const unordered_set<TableIndex> &right_bindings,
                                                                  CorrelatedColumns &correlated_columns) {
	ExternalExpressionDepthIncreaser depth_increaser(correlated_columns);
	depth_increaser.Increase(lateral_child);

	PairDependentJoinConditionCorrelator correlator(left_bindings, right_bindings, correlated_columns);
	correlator.VisitExpression(&condition);
	return !correlated_columns.empty();
}

} // namespace duckdb
