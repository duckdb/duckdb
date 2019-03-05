#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/list.hpp"
#include "parser/query_node/list.hpp"
#include "parser/statement/list.hpp"
#include "parser/tableref/list.hpp"
#include "planner/binder.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"
#include "planner/operator/logical_any_join.hpp"

#include <map>

using namespace duckdb;
using namespace std;

JoinSide LogicalComparisonJoin::CombineJoinSide(JoinSide left, JoinSide right) {
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

JoinSide LogicalComparisonJoin::GetJoinSide(size_t table_binding, unordered_set<size_t> &left_bindings,
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

JoinSide LogicalComparisonJoin::GetJoinSide(Expression &expression, unordered_set<size_t> &left_bindings,
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

JoinSide LogicalComparisonJoin::GetJoinSide(unordered_set<size_t> bindings, unordered_set<size_t> &left_bindings,
                                            unordered_set<size_t> &right_bindings) {
	JoinSide side = JoinSide::NONE;
	for (auto binding : bindings) {
		side = CombineJoinSide(side, GetJoinSide(binding, left_bindings, right_bindings));
	}
	return side;
}

//! Create a JoinCondition from a comparison
static bool CreateJoinCondition(Expression &expr, unordered_set<size_t> &left_bindings,
                                unordered_set<size_t> &right_bindings, vector<JoinCondition> &conditions) {
	// comparison
	auto &comparison = (ComparisonExpression &)expr;
	auto left_side = LogicalComparisonJoin::GetJoinSide(*comparison.left, left_bindings, right_bindings);
	auto right_side = LogicalComparisonJoin::GetJoinSide(*comparison.right, left_bindings, right_bindings);
	if (left_side != JoinSide::BOTH && right_side != JoinSide::BOTH) {
		// join condition can be divided in a left/right side
		JoinCondition condition;
		condition.comparison = expr.type;
		auto left = move(comparison.left);
		auto right = move(comparison.right);
		if (left_side == JoinSide::RIGHT) {
			// left = right, right = left, flip the comparison symbol and reverse sides
			swap(left, right);
			condition.comparison = ComparisonExpression::FlipComparisionExpression(expr.type);
		}
		condition.left = move(left);
		condition.right = move(right);
		conditions.push_back(move(condition));
		return true;
	}
	return false;
}

unique_ptr<Expression> LogicalComparisonJoin::CreateExpressionFromCondition(JoinCondition cond) {
	return make_unique<ComparisonExpression>(cond.comparison, move(cond.left), move(cond.right));
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::CreateJoin(JoinType type, unique_ptr<LogicalOperator> left_child,
                                                              unique_ptr<LogicalOperator> right_child,
                                                              unordered_set<size_t> &left_bindings,
                                                              unordered_set<size_t> &right_bindings,
                                                              vector<unique_ptr<Expression>> &expressions) {
	vector<JoinCondition> conditions;
	vector<unique_ptr<Expression>> arbitrary_expressions;
	// first check if we can create
	for (size_t i = 0; i < expressions.size(); i++) {
		auto &expr = expressions[i];
		auto total_side = GetJoinSide(*expr, left_bindings, right_bindings);
		if (total_side != JoinSide::BOTH) {
			// join condition does not reference both sides, add it as filter under the join
			if (type == JoinType::LEFT && total_side == JoinSide::RIGHT) {
				// filter is on RHS and the join is a LEFT OUTER join, we can push it in the right child
				if (right_child->type != LogicalOperatorType::FILTER) {
					// not a filter yet, push a new empty filter
					auto filter = make_unique<LogicalFilter>();
					filter->AddChild(move(right_child));
					right_child = move(filter);
				}
				// push the expression into the filter
				auto &filter = (LogicalFilter &)*right_child;
				filter.expressions.push_back(move(expr));
				continue;
			}
		} else if (expr->type >= ExpressionType::COMPARE_EQUAL &&
		           expr->type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			// comparison, check if we can create a comparison JoinCondition
			if (CreateJoinCondition(*expr, left_bindings, right_bindings, conditions)) {
				// successfully created the join condition
				continue;
			}
		} else if (expr->type == ExpressionType::OPERATOR_NOT) {
			auto &not_expr = (OperatorExpression &)*expr;
			assert(not_expr.children.size() == 1);
			ExpressionType child_type = not_expr.children[0]->GetExpressionType();
			// the condition is ON NOT (EXPRESSION)
			// we can transform this to remove the NOT if the child is a Comparison
			// e.g.:
			// ON NOT (X = 3) can be turned into ON (X <> 3)
			// ON NOT (X > 3) can be turned into ON (X <= 3)
			// for non-comparison operators here we just push the filter
			if (child_type >= ExpressionType::COMPARE_EQUAL &&
			    child_type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				// switcheroo the child condition
				// our join needs to compare explicit left and right sides. So we
				// invert the condition to express NOT, this way we can still use
				// equi-joins
				not_expr.children[0]->type = ComparisonExpression::NegateComparisionExpression(child_type);
				if (CreateJoinCondition(*not_expr.children[0], left_bindings, right_bindings, conditions)) {
					// successfully created the join condition
					continue;
				}
			}
		}
		arbitrary_expressions.push_back(move(expr));
	}
	if (conditions.size() > 0) {
		// we successfully convertedexpressions into JoinConditions
		// create a LogicalComparisonJoin
		auto comp_join = make_unique<LogicalComparisonJoin>(type);
		comp_join->conditions = move(conditions);
		comp_join->children.push_back(move(left_child));
		comp_join->children.push_back(move(right_child));
		if (arbitrary_expressions.size() > 0) {
			// we have some arbitrary expressions as well
			// add them to a filter
			auto filter = make_unique<LogicalFilter>();
			for (auto &expr : arbitrary_expressions) {
				filter->expressions.push_back(move(expr));
			}
			LogicalFilter::SplitPredicates(filter->expressions);
			filter->children.push_back(move(comp_join));
			return move(filter);
		}
		return move(comp_join);
	} else if (arbitrary_expressions.size() > 0) {
		// if we get here we could not create any JoinConditions
		// turn this into an arbitrary expression join
		auto any_join = make_unique<LogicalAnyJoin>(type);
		// create the condition
		any_join->children.push_back(move(left_child));
		any_join->children.push_back(move(right_child));
		// AND all the arbitrary expressions together
		// do the same with any remaining conditions
		any_join->condition = move(arbitrary_expressions[0]);
		for (size_t i = 1; i < arbitrary_expressions.size(); i++) {
			any_join->condition = make_unique<ConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, move(any_join->condition), move(arbitrary_expressions[i]));
		}
		return move(any_join);
	} else {
		// all conditions were pushed down, transform into cross product
		auto cross_product = make_unique<LogicalCrossProduct>();
		cross_product->children.push_back(move(left_child));
		cross_product->children.push_back(move(right_child));
		return move(cross_product);
	}
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

	// non inner-join, create the the actual join
	AcceptChild(&expr.left);
	auto left_child = move(root);
	AcceptChild(&expr.right);
	auto right_child = move(root);

	// split the expressions by the AND clause
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(move(expr.condition));
	LogicalFilter::SplitPredicates(expressions);

	// find the table bindings on the LHS and RHS of the join
	unordered_set<size_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*left_child, left_bindings);
	LogicalJoin::GetTableReferences(*right_child, right_bindings);
	// now create the join operator from the set of join conditions
	auto result = LogicalComparisonJoin::CreateJoin(expr.type, move(left_child), move(right_child), left_bindings,
	                                                right_bindings, expressions);
	LogicalOperator *join;
	if (result->type == LogicalOperatorType::FILTER) {
		join = result->children[0].get();
	} else {
		join = result.get();
	}

	// we visit the expressions depending on the type of join
	if (join->type == LogicalOperatorType::COMPARISON_JOIN) {
		// comparison join
		// in this join we visit the expressions on the LHS with the LHS as root node
		// and the expressions on the RHS with the RHS as root node
		auto &comp_join = (LogicalComparisonJoin &)*join;

		// first visit the left conditions
		root = move(comp_join.children[0]);
		for (size_t i = 0; i < comp_join.conditions.size(); i++) {
			VisitExpression(&comp_join.conditions[i].left);
		}
		comp_join.children[0] = move(root);
		// now visit the right conditions
		root = move(comp_join.children[1]);
		for (size_t i = 0; i < comp_join.conditions.size(); i++) {
			VisitExpression(&comp_join.conditions[i].right);
		}
		comp_join.children[1] = move(root);
		// move the join as the root node
		root = move(result);
	} else if (join->type == LogicalOperatorType::ANY_JOIN) {
		auto &any_join = (LogicalAnyJoin &)*join;
		// for the any join we just visit the condition
		root = move(result);
		if (any_join.condition->HasSubquery()) {
			throw NotImplementedException("Cannot perform non-inner join on subquery!");
		}
		VisitExpression(&any_join.condition);
	} else {
		// no conditions left: must be a cross product
		assert(join->type == LogicalOperatorType::CROSS_PRODUCT);
		root = move(result);
	}
	return nullptr;
}
