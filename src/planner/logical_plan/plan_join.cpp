#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/list.hpp"
#include "parser/query_node/list.hpp"
#include "parser/statement/list.hpp"
#include "parser/tableref/list.hpp"
#include "planner/binder.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

#include <map>

using namespace duckdb;
using namespace std;

static JoinSide CombineJoinSide(JoinSide left, JoinSide right) {
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

static JoinSide GetJoinSide(size_t table_binding, unordered_set<size_t> &left_bindings,
                            unordered_set<size_t> &right_bindings) {
	if (left_bindings.find(table_binding) != left_bindings.end()) {
		// column references table on left side
		assert(right_bindings.find(table_binding) == right_bindings.end());
		return JoinSide::LEFT;
	} else {
		// column references table on right side
		assert(right_bindings.find(table_binding) != right_bindings.end());
		return JoinSide::RIGHT;
	}
}

static JoinSide GetJoinSide(Expression &expression, unordered_set<size_t> &left_bindings,
                            unordered_set<size_t> &right_bindings) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expression;
		if (colref.depth > 0) {
			throw Exception("Non-inner join on correlated columns not supported");
		}
		return GetJoinSide(colref.binding.table_index, left_bindings, right_bindings);
	}
	assert(expression.type != ExpressionType::BOUND_REF);
	if (expression.type == ExpressionType::SUBQUERY) {
		assert(expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY);
		auto &subquery = (BoundSubqueryExpression &)expression;
		// correlated subquery, check the side of each of correlated columns in the subquery
		JoinSide side = JoinSide::NONE;
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
	expression.EnumerateChildren([&](Expression *child) {
		auto child_side = GetJoinSide(*child, left_bindings, right_bindings);
		join_side = CombineJoinSide(child_side, join_side);
	});
	return join_side;
}

static void CreateJoinCondition(LogicalComparisonJoin &join, unique_ptr<Expression> expr, unordered_set<size_t> &left_bindings,
                                unordered_set<size_t> &right_bindings) {
	auto total_side = GetJoinSide(*expr, left_bindings, right_bindings);
	if (total_side != JoinSide::BOTH) {
		// join condition does not reference both sides, add it as filter under the join
		if (join.type == JoinType::LEFT && total_side == JoinSide::RIGHT) {
			// filter is on RHS and the join is a LEFT OUTER join, we can push it in the right child
			if (join.children[1]->type != LogicalOperatorType::FILTER) {
				// not a filter yet, push a new empty filter
				auto filter = make_unique<LogicalFilter>();
				filter->AddChild(move(join.children[1]));
				join.children[1] = move(filter);
			}
			// push the expression into the filter
			auto &filter = (LogicalFilter &)*join.children[1];
			filter.expressions.push_back(move(expr));
			return;
		}
		// cannot push expression as filter
		// push it as an arbitrary expression
	} else if (expr->type >= ExpressionType::COMPARE_EQUAL &&
	           expr->type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		// comparison
		auto &comparison = (ComparisonExpression &)*expr;
		auto left_side = GetJoinSide(*comparison.left, left_bindings, right_bindings);
		auto right_side = GetJoinSide(*comparison.right, left_bindings, right_bindings);
		if (left_side != JoinSide::BOTH && right_side != JoinSide::BOTH) {
			// join condition can be divided in a left/right side
			JoinCondition condition;
			condition.comparison = expr->type;
			auto left = move(comparison.left);
			auto right = move(comparison.right);
			if (left_side == JoinSide::RIGHT) {
				// left = right, right = left, flip the comparison symbol and reverse sides
				swap(left, right);
				condition.comparison = ComparisonExpression::FlipComparisionExpression(expr->type);
			}
			condition.left = move(left);
			condition.right = move(right);
			join.conditions.push_back(move(condition));
			return;
		}
	} else if (expr->type == ExpressionType::OPERATOR_NOT) {
		auto &op_expr = (OperatorExpression &)*expr;
		assert(op_expr.children.size() == 1);
		ExpressionType child_type = op_expr.children[0]->GetExpressionType();
		// the condition is ON NOT (EXPRESSION)
		// we can transform this to remove the NOT if the child is a Comparison
		// e.g.:
		// ON NOT (X = 3) can be turned into ON (X <> 3)
		// ON NOT (X > 3) can be turned into ON (X <= 3)
		// for non-comparison operators here we just push the filter
		if (child_type >= ExpressionType::COMPARE_EQUAL && child_type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			// switcheroo the child condition
			// our join needs to compare explicit left and right sides. So we
			// invert the condition to express NOT, this way we can still use
			// equi-joins
			op_expr.children[0]->type = ComparisonExpression::NegateComparisionExpression(child_type);
			return CreateJoinCondition(join, move(op_expr.children[0]), left_bindings, right_bindings);
		}
	}
	// cannot create a proper join condition
	// push an arbitrary expression join
	throw NotImplementedException("FIXME: arbitrary expression join!");
	JoinCondition cond;
	cond.left = move(expr);
	cond.comparison = ExpressionType::INVALID;
	join.conditions.push_back(move(cond));
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(JoinRef &expr) {
	if (root) {
		throw Exception("Joins need to be the root");
	}

	if (expr.type == JoinType::INNER) {
		// inner join, generate a cross product + filter
		// this will be later turned into a proper join by the join order optimizer
		auto cross_product = make_unique<LogicalCrossProduct>();

		AcceptChild(&expr.left);
		cross_product->AddChild(move(root));

		AcceptChild(&expr.right);
		cross_product->AddChild(move(root));
		root = move(cross_product);

		auto filter = make_unique<LogicalFilter>(move(expr.condition));
		// visit the expressions in the filter
		for (size_t i = 0; i < filter->expressions.size(); i++) {
			VisitExpression(&filter->expressions[i]);
		}
		filter->AddChild(move(root));
		root = move(filter);

		return nullptr;
	}

	// non inner-join
	// create the the actual join
	auto join = make_unique<LogicalComparisonJoin>(expr.type);

	AcceptChild(&expr.left);
	join->AddChild(move(root));

	AcceptChild(&expr.right);
	join->AddChild(move(root));

	// now split the expressions by the AND clause
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(move(expr.condition));
	LogicalFilter::SplitPredicates(expressions);

	// find the table bindings on the LHS and RHS of the join
	unordered_set<size_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*join->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*join->children[1], right_bindings);
	// for each expression turn it into a proper JoinCondition
	for (size_t i = 0; i < expressions.size(); i++) {
		CreateJoinCondition(*join, move(expressions[i]), left_bindings, right_bindings);
	}
	// first visit the left conditions
	root = move(join->children[0]);
	for (size_t i = 0; i < join->conditions.size(); i++) {
		VisitExpression(&join->conditions[i].left);
	}
	join->children[0] = move(root);
	// now visit the right conditions
	root = move(join->children[1]);
	for (size_t i = 0; i < join->conditions.size(); i++) {
		VisitExpression(&join->conditions[i].right);
	}
	join->children[1] = move(root);
	root = move(join);
	return nullptr;
}
