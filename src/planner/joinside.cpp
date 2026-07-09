#include "duckdb/planner/joinside.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

JoinCondition JoinCondition::Copy() const {
	if (IsComparison()) {
		JoinCondition copy(left->Copy(), right->Copy(), comparison);
		if (left_stats) {
			copy.SetLeftStats(left_stats->ToUnique());
		}
		if (right_stats) {
			copy.SetRightStats(right_stats->ToUnique());
		}
		return copy;
	}

	JoinCondition copy(left->Copy());
	const auto &expr_stats = GetExpressionStats();
	if (expr_stats) {
		copy.SetExpressionStats(expr_stats->ToUnique());
	}
	return copy;
}

unique_ptr<Expression> JoinCondition::CreateExpression(JoinCondition cond) {
	if (cond.IsComparison()) {
		auto bound_comparison = BoundComparisonExpression::Create(
		    cond.GetComparisonType(), std::move(cond.LeftReference()), std::move(cond.RightReference()));
		return bound_comparison;
	}
	return std::move(cond.JoinExpressionReference());
}

unique_ptr<Expression> JoinCondition::CreateExpression(vector<JoinCondition> conditions) {
	unique_ptr<Expression> result;
	for (auto &cond : conditions) {
		auto expr = CreateExpression(std::move(cond));
		if (!result) {
			result = std::move(expr);
		} else {
			auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(expr),
			                                                  std::move(result));
			result = std::move(conj);
		}
	}
	return result;
}

JoinSide JoinSide::CombineJoinSide(JoinSide left, JoinSide right) {
	if (left == JoinSide::NONE) {
		return right;
	}
	if (right == JoinSide::NONE) {
		return left;
	}
	if (left != right) {
		return JoinSide::BOTH;
	}
	return left;
}

JoinSide JoinSide::GetJoinSide(TableIndex table_binding, const unordered_set<TableIndex> &left_bindings,
                               const unordered_set<TableIndex> &right_bindings) {
	if (left_bindings.find(table_binding) != left_bindings.end()) {
		// column references table on left side
		D_ASSERT(right_bindings.find(table_binding) == right_bindings.end());
		return JoinSide::LEFT;
	} else {
		// column references table on right side
		D_ASSERT(right_bindings.find(table_binding) != right_bindings.end());
		return JoinSide::RIGHT;
	}
}

JoinSide JoinSide::GetJoinSide(const Expression &expression, const unordered_set<TableIndex> &left_bindings,
                               const unordered_set<TableIndex> &right_bindings) {
	if (expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		if (colref.Depth() > 0) {
			return JoinSide::NONE;
		}
		return GetJoinSide(colref.Binding().table_index, left_bindings, right_bindings);
	}
	D_ASSERT(expression.GetExpressionType() != ExpressionType::BOUND_REF);
	if (expression.GetExpressionType() == ExpressionType::SUBQUERY) {
		D_ASSERT(expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY);
		auto &subquery = expression.Cast<BoundSubqueryExpression>();
		JoinSide side = JoinSide::NONE;
		for (auto &child : subquery.GetChildren()) {
			auto child_side = GetJoinSide(*child, left_bindings, right_bindings);
			side = CombineJoinSide(side, child_side);
		}
		// correlated subquery, check the side of each of correlated columns in the subquery
		for (auto &corr : subquery.GetBinder()->correlated_columns) {
			if (corr.depth > 1) {
				// correlated column has depth > 1
				// it does not refer to any table in the current set of bindings
				return JoinSide::BOTH;
			}
			auto correlated_side = GetJoinSide(corr.binding.table_index, left_bindings, right_bindings);
			side = CombineJoinSide(side, correlated_side);
		}
		return side;
	}
	JoinSide join_side = JoinSide::NONE;
	ExpressionIterator::EnumerateChildren(expression, [&](const Expression &child) {
		auto child_side = GetJoinSide(child, left_bindings, right_bindings);
		join_side = CombineJoinSide(child_side, join_side);
	});
	return join_side;
}

JoinSide JoinSide::GetCurrentJoinSide(TableIndex table_binding, const unordered_set<TableIndex> &left_bindings,
                                      const unordered_set<TableIndex> &right_bindings) {
	if (left_bindings.find(table_binding) != left_bindings.end()) {
		D_ASSERT(right_bindings.find(table_binding) == right_bindings.end());
		return JoinSide::LEFT;
	}
	if (right_bindings.find(table_binding) != right_bindings.end()) {
		return JoinSide::RIGHT;
	}
	return JoinSide::NONE;
}

JoinSide JoinSide::GetCurrentJoinSide(const Expression &expression, const unordered_set<TableIndex> &left_bindings,
                                      const unordered_set<TableIndex> &right_bindings) {
	if (expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		if (colref.Depth() > 0) {
			return JoinSide::NONE;
		}
		return GetCurrentJoinSide(colref.Binding().table_index, left_bindings, right_bindings);
	}
	D_ASSERT(expression.GetExpressionType() != ExpressionType::BOUND_REF);
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		auto &subquery = expression.Cast<BoundSubqueryExpression>();
		JoinSide side = JoinSide::NONE;
		for (auto &child : subquery.GetChildren()) {
			auto child_side = GetCurrentJoinSide(*child, left_bindings, right_bindings);
			side = CombineJoinSide(side, child_side);
		}
		for (auto &corr : subquery.GetBinder()->correlated_columns) {
			if (corr.depth > 1) {
				continue;
			}
			auto correlated_side = GetCurrentJoinSide(corr.binding.table_index, left_bindings, right_bindings);
			side = CombineJoinSide(side, correlated_side);
		}
		return side;
	}
	JoinSide join_side = JoinSide::NONE;
	ExpressionIterator::EnumerateChildren(expression, [&](const Expression &child) {
		auto child_side = GetCurrentJoinSide(child, left_bindings, right_bindings);
		join_side = CombineJoinSide(join_side, child_side);
	});
	return join_side;
}

static void AddCurrentCorrelatedColumnSides(JoinSide &side, CorrelatedColumns *columns,
                                            const unordered_set<TableIndex> &left_bindings,
                                            const unordered_set<TableIndex> &right_bindings) {
	if (!columns) {
		return;
	}
	for (auto &corr : *columns) {
		auto corr_side = JoinSide::GetCurrentJoinSide(corr.binding.table_index, left_bindings, right_bindings);
		side = JoinSide::CombineJoinSide(side, corr_side);
	}
}

static CorrelatedColumns *GetOperatorCorrelatedColumns(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
		return &op.Cast<LogicalDependentJoin>().correlated_columns;
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		return &op.Cast<LogicalCTE>().correlated_columns;
	default:
		return nullptr;
	}
}

static JoinSide GetOperatorCurrentJoinSide(LogicalOperator &op, const unordered_set<TableIndex> &left_bindings,
                                           const unordered_set<TableIndex> &right_bindings) {
	JoinSide side = JoinSide::NONE;
	AddCurrentCorrelatedColumnSides(side, GetOperatorCorrelatedColumns(op), left_bindings, right_bindings);
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
		if (!expr || !*expr) {
			return;
		}
		auto expr_side = JoinSide::GetCurrentJoinSide(**expr, left_bindings, right_bindings);
		side = JoinSide::CombineJoinSide(side, expr_side);
	});
	for (auto &child : op.children) {
		auto child_side = GetOperatorCurrentJoinSide(*child, left_bindings, right_bindings);
		side = JoinSide::CombineJoinSide(side, child_side);
	}
	return side;
}

static JoinSide GetSubqueryCurrentJoinSide(BoundSubqueryExpression &subquery,
                                           const unordered_set<TableIndex> &left_bindings,
                                           const unordered_set<TableIndex> &right_bindings) {
	auto side = JoinSide::GetCurrentJoinSide(subquery, left_bindings, right_bindings);
	if (subquery.SubqueryMutable().plan) {
		auto plan_side = GetOperatorCurrentJoinSide(*subquery.SubqueryMutable().plan, left_bindings, right_bindings);
		side = JoinSide::CombineJoinSide(side, plan_side);
	}
	return side;
}

bool JoinSide::HasPairDependentSubquery(Expression &expression, const unordered_set<TableIndex> &left_bindings,
                                        const unordered_set<TableIndex> &right_bindings) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		auto side =
		    GetSubqueryCurrentJoinSide(expression.Cast<BoundSubqueryExpression>(), left_bindings, right_bindings);
		if (side == JoinSide::BOTH) {
			return true;
		}
	}
	bool result = false;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &child) {
		if (!result && HasPairDependentSubquery(child, left_bindings, right_bindings)) {
			result = true;
		}
	});
	return result;
}

JoinSide JoinSide::GetJoinSide(const unordered_set<TableIndex> &bindings,
                               const unordered_set<TableIndex> &left_bindings,
                               const unordered_set<TableIndex> &right_bindings) {
	JoinSide side = JoinSide::NONE;
	for (auto binding : bindings) {
		side = CombineJoinSide(side, GetJoinSide(binding, left_bindings, right_bindings));
	}
	return side;
}

} // namespace duckdb
