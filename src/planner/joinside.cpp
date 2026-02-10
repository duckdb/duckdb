#include "duckdb/planner/joinside.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

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
		auto bound_comparison = make_uniq<BoundComparisonExpression>(
		    cond.GetComparisonType(), std::move(cond.LeftReference()), std::move(cond.RightReference()));
		return std::move(bound_comparison);
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

bool JoinCondition::Compare(const JoinCondition &a, const JoinCondition &b) {
	//	Comparisons come before non-comparisons
	if (!a.IsComparison()) {
		return !b.IsComparison();
	} else if (!b.IsComparison()) {
		return true;
	}

	//	Both are comparisons, so use distinct counts to compare selectivities
	//	(higher is more selective, zero is unknown/completely unselective)
	const auto a_left = a.GetLeftStats() ? a.GetLeftStats()->GetDistinctCount() : 0;
	const auto a_right = a.GetRightStats() ? a.GetRightStats()->GetDistinctCount() : 0;
	const auto a_min = MinValue(a_left, a_right);
	const auto a_type = a.right->return_type.InternalType();

	const auto b_left = b.GetLeftStats() ? b.GetLeftStats()->GetDistinctCount() : 0;
	const auto b_right = b.GetRightStats() ? b.GetRightStats()->GetDistinctCount() : 0;
	const auto b_min = MinValue(b_left, b_right);
	const auto b_type = b.right->return_type.InternalType();

	switch (a.GetComparisonType()) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		switch (b.GetComparisonType()) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			//	Prefer higher rhs selectivities for equalities
			if (a_right != b_right) {
				return a_right > b_right;
			}
			//	Prefer narrower types for faster comparisons
			if (!TypeIsConstantSize(a_type)) {
				return !TypeIsConstantSize(b_type);
			} else if (!TypeIsConstantSize(b_type)) {
				return true;
			}
			return GetTypeIdSize(a_type) < GetTypeIdSize(b_type);
		default:
			//	Prefer equalities
			return true;
		}
	default:
		switch (b.GetComparisonType()) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			//	Equality before inequality
			return false;
		default:
			//	Prefer higher selectivities, but use the minimum of both sides
			if (a_min != b_min) {
				return a_min > b_min;
			}
			//	Prefer narrower types for faster comparisons
			if (!TypeIsConstantSize(a_type)) {
				return !TypeIsConstantSize(b_type);
			} else if (!TypeIsConstantSize(b_type)) {
				return true;
			}
			return GetTypeIdSize(a_type) < GetTypeIdSize(b_type);
		}
	}
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

JoinSide JoinSide::GetJoinSide(idx_t table_binding, const unordered_set<idx_t> &left_bindings,
                               const unordered_set<idx_t> &right_bindings) {
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

JoinSide JoinSide::GetJoinSide(Expression &expression, const unordered_set<idx_t> &left_bindings,
                               const unordered_set<idx_t> &right_bindings) {
	if (expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		if (colref.depth > 0) {
			throw NotImplementedException("Non-inner join on correlated columns not supported");
		}
		return GetJoinSide(colref.binding.table_index, left_bindings, right_bindings);
	}
	D_ASSERT(expression.GetExpressionType() != ExpressionType::BOUND_REF);
	if (expression.GetExpressionType() == ExpressionType::SUBQUERY) {
		D_ASSERT(expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY);
		auto &subquery = expression.Cast<BoundSubqueryExpression>();
		JoinSide side = JoinSide::NONE;
		for (auto &child : subquery.children) {
			auto child_side = GetJoinSide(*child, left_bindings, right_bindings);
			side = CombineJoinSide(side, child_side);
		}
		// correlated subquery, check the side of each of correlated columns in the subquery
		for (auto &corr : subquery.binder->correlated_columns) {
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
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &child) {
		auto child_side = GetJoinSide(child, left_bindings, right_bindings);
		join_side = CombineJoinSide(child_side, join_side);
	});
	return join_side;
}

JoinSide JoinSide::GetJoinSide(const unordered_set<idx_t> &bindings, const unordered_set<idx_t> &left_bindings,
                               const unordered_set<idx_t> &right_bindings) {
	JoinSide side = JoinSide::NONE;
	for (auto binding : bindings) {
		side = CombineJoinSide(side, GetJoinSide(binding, left_bindings, right_bindings));
	}
	return side;
}

} // namespace duckdb
