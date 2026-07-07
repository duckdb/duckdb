#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"

namespace duckdb {

static void AddLateralCorrelation(CorrelatedColumns &correlated_columns, CorrelatedColumnInfo info) {
	for (auto &existing : correlated_columns) {
		if (existing == info) {
			return;
		}
	}
	correlated_columns.AddColumn(std::move(info));
}

static void AddLateralCorrelation(CorrelatedColumns &correlated_columns, const BoundColumnRefExpression &colref) {
	AddLateralCorrelation(correlated_columns,
	                      CorrelatedColumnInfo(colref.Binding(), colref.GetReturnType(), colref.GetName(),
	                                           colref.Depth() + 1));
}

static bool IsBindingIn(const ColumnBinding &binding, const unordered_set<TableIndex> &bindings) {
	return bindings.find(binding.table_index) != bindings.end();
}

class LateralizeJoinCondition : public LogicalOperatorVisitor {
public:
	explicit LateralizeJoinCondition(const unordered_set<TableIndex> &left_bindings,
	                                 const unordered_set<TableIndex> &right_bindings,
	                                 CorrelatedColumns &correlated_columns)
	    : left_bindings(left_bindings), right_bindings(right_bindings), correlated_columns(correlated_columns) {
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (!IsBindingIn(expr.Binding(), right_bindings) &&
		    (IsBindingIn(expr.Binding(), left_bindings) || expr.Depth() > 0)) {
			AddLateralCorrelation(correlated_columns, expr);
			expr.DepthMutable()++;
		}
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		for (auto &corr : expr.GetBinder()->correlated_columns) {
			if (IsBindingIn(corr.binding, right_bindings) ||
			    (!IsBindingIn(corr.binding, left_bindings) && corr.depth <= 1)) {
				continue;
			}
			AddLateralCorrelation(correlated_columns, corr);
			corr.depth++;
		}
		// The join condition becomes a filter on the RHS of a left lateral join.
		// Existing references to anything outside the RHS therefore cross one more binder boundary.
		VisitOperator(*expr.SubqueryMutable().plan);
		return nullptr;
	}

private:
	const unordered_set<TableIndex> &left_bindings;
	const unordered_set<TableIndex> &right_bindings;
	CorrelatedColumns &correlated_columns;
};

static bool HasPairDependentSubquery(const Expression &expr, const unordered_set<TableIndex> &left_bindings,
                                     const unordered_set<TableIndex> &right_bindings) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		auto side = JoinSide::GetCurrentJoinSide(expr, left_bindings, right_bindings);
		if (side == JoinSide::BOTH) {
			return true;
		}
	}
	bool has_pair_dependent_subquery = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (HasPairDependentSubquery(child, left_bindings, right_bindings)) {
			has_pair_dependent_subquery = true;
		}
	});
	return has_pair_dependent_subquery;
}

bool Binder::TryPlanPairDependentLeftJoin(BoundJoinRef &ref, unique_ptr<LogicalOperator> &left,
                                          unique_ptr<LogicalOperator> &right, unique_ptr<LogicalOperator> &result) {
	if (!ref.condition || ref.lateral || ref.type != JoinType::LEFT || ref.ref_type != JoinRefType::REGULAR) {
		return false;
	}
	unordered_set<TableIndex> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*left, left_bindings);
	LogicalJoin::GetTableReferences(*right, right_bindings);
	if (!HasPairDependentSubquery(*ref.condition, left_bindings, right_bindings)) {
		return false;
	}

	CorrelatedColumns correlated_columns;
	LateralizeJoinCondition lateralize(left_bindings, right_bindings, correlated_columns);
	lateralize.VisitExpression(&ref.condition);
	if (correlated_columns.empty()) {
		return false;
	}

	auto filter = make_uniq<LogicalFilter>(std::move(ref.condition));
	filter->AddChild(std::move(right));
	right = std::move(filter);
	result = PlanLateralJoin(std::move(left), std::move(right), correlated_columns, JoinType::LEFT, nullptr);
	return true;
}

} // namespace duckdb
