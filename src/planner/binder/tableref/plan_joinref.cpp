#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression_binder/lateral_binder.hpp"

namespace duckdb {

//! Create a JoinCondition from a comparison
static bool CreateJoinCondition(Expression &expr, const unordered_set<idx_t> &left_bindings,
                                const unordered_set<idx_t> &right_bindings, vector<JoinCondition> &conditions) {
	// comparison
	auto &comparison = (BoundComparisonExpression &)expr;
	auto left_side = JoinSide::GetJoinSide(*comparison.left, left_bindings, right_bindings);
	auto right_side = JoinSide::GetJoinSide(*comparison.right, left_bindings, right_bindings);
	if (left_side != JoinSide::BOTH && right_side != JoinSide::BOTH) {
		// join condition can be divided in a left/right side
		JoinCondition condition;
		condition.comparison = expr.type;
		auto left = std::move(comparison.left);
		auto right = std::move(comparison.right);
		if (left_side == JoinSide::RIGHT) {
			// left = right, right = left, flip the comparison symbol and reverse sides
			swap(left, right);
			condition.comparison = FlipComparisionExpression(expr.type);
		}
		condition.left = std::move(left);
		condition.right = std::move(right);
		conditions.push_back(std::move(condition));
		return true;
	}
	return false;
}

void LogicalComparisonJoin::ExtractJoinConditions(JoinType type, unique_ptr<LogicalOperator> &left_child,
                                                  unique_ptr<LogicalOperator> &right_child,
                                                  const unordered_set<idx_t> &left_bindings,
                                                  const unordered_set<idx_t> &right_bindings,
                                                  vector<unique_ptr<Expression>> &expressions,
                                                  vector<JoinCondition> &conditions,
                                                  vector<unique_ptr<Expression>> &arbitrary_expressions) {
	for (auto &expr : expressions) {
		auto total_side = JoinSide::GetJoinSide(*expr, left_bindings, right_bindings);
		if (total_side != JoinSide::BOTH) {
			// join condition does not reference both sides, add it as filter under the join
			if (type == JoinType::LEFT && total_side == JoinSide::RIGHT) {
				// filter is on RHS and the join is a LEFT OUTER join, we can push it in the right child
				if (right_child->type != LogicalOperatorType::LOGICAL_FILTER) {
					// not a filter yet, push a new empty filter
					auto filter = make_unique<LogicalFilter>();
					filter->AddChild(std::move(right_child));
					right_child = std::move(filter);
				}
				// push the expression into the filter
				auto &filter = (LogicalFilter &)*right_child;
				filter.expressions.push_back(std::move(expr));
				continue;
			}
		} else if ((expr->type >= ExpressionType::COMPARE_EQUAL &&
		            expr->type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) ||
		           expr->type == ExpressionType::COMPARE_DISTINCT_FROM ||
		           expr->type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			// comparison, check if we can create a comparison JoinCondition
			if (CreateJoinCondition(*expr, left_bindings, right_bindings, conditions)) {
				// successfully created the join condition
				continue;
			}
		}
		arbitrary_expressions.push_back(std::move(expr));
	}
}

void LogicalComparisonJoin::ExtractJoinConditions(JoinType type, unique_ptr<LogicalOperator> &left_child,
                                                  unique_ptr<LogicalOperator> &right_child,
                                                  vector<unique_ptr<Expression>> &expressions,
                                                  vector<JoinCondition> &conditions,
                                                  vector<unique_ptr<Expression>> &arbitrary_expressions) {
	unordered_set<idx_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*left_child, left_bindings);
	LogicalJoin::GetTableReferences(*right_child, right_bindings);
	return ExtractJoinConditions(type, left_child, right_child, left_bindings, right_bindings, expressions, conditions,
	                             arbitrary_expressions);
}

void LogicalComparisonJoin::ExtractJoinConditions(JoinType type, unique_ptr<LogicalOperator> &left_child,
                                                  unique_ptr<LogicalOperator> &right_child,
                                                  unique_ptr<Expression> condition, vector<JoinCondition> &conditions,
                                                  vector<unique_ptr<Expression>> &arbitrary_expressions) {
	// split the expressions by the AND clause
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(std::move(condition));
	LogicalFilter::SplitPredicates(expressions);
	return ExtractJoinConditions(type, left_child, right_child, expressions, conditions, arbitrary_expressions);
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::CreateJoin(JoinType type, unique_ptr<LogicalOperator> left_child,
                                                              unique_ptr<LogicalOperator> right_child,
                                                              vector<JoinCondition> conditions,
                                                              vector<unique_ptr<Expression>> arbitrary_expressions) {
	bool need_to_consider_arbitrary_expressions = true;
	if (type == JoinType::INNER) {
		// for inner joins we can push arbitrary expressions as a filter
		// here we prefer to create a comparison join if possible
		// that way we can use the much faster hash join to process the main join
		// rather than doing a nested loop join to handle arbitrary expressions

		// for left and full outer joins we HAVE to process all join conditions
		// because pushing a filter will lead to an incorrect result, as non-matching tuples cannot be filtered out
		need_to_consider_arbitrary_expressions = false;
	}
	if ((need_to_consider_arbitrary_expressions && !arbitrary_expressions.empty()) || conditions.empty()) {
		if (arbitrary_expressions.empty()) {
			// all conditions were pushed down, add TRUE predicate
			arbitrary_expressions.push_back(make_unique<BoundConstantExpression>(Value::BOOLEAN(true)));
		}
		for (auto &condition : conditions) {
			arbitrary_expressions.push_back(JoinCondition::CreateExpression(std::move(condition)));
		}
		// if we get here we could not create any JoinConditions
		// turn this into an arbitrary expression join
		auto any_join = make_unique<LogicalAnyJoin>(type);
		// create the condition
		any_join->children.push_back(std::move(left_child));
		any_join->children.push_back(std::move(right_child));
		// AND all the arbitrary expressions together
		// do the same with any remaining conditions
		any_join->condition = std::move(arbitrary_expressions[0]);
		for (idx_t i = 1; i < arbitrary_expressions.size(); i++) {
			any_join->condition = make_unique<BoundConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, std::move(any_join->condition), std::move(arbitrary_expressions[i]));
		}
		return std::move(any_join);
	} else {
		// we successfully converted expressions into JoinConditions
		// create a LogicalComparisonJoin
		auto comp_join = make_unique<LogicalComparisonJoin>(type);
		comp_join->conditions = std::move(conditions);
		comp_join->children.push_back(std::move(left_child));
		comp_join->children.push_back(std::move(right_child));
		if (!arbitrary_expressions.empty()) {
			// we have some arbitrary expressions as well
			// add them to a filter
			auto filter = make_unique<LogicalFilter>();
			for (auto &expr : arbitrary_expressions) {
				filter->expressions.push_back(std::move(expr));
			}
			LogicalFilter::SplitPredicates(filter->expressions);
			filter->children.push_back(std::move(comp_join));
			return std::move(filter);
		}
		return std::move(comp_join);
	}
}

static bool HasCorrelatedColumns(Expression &expression) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expression;
		if (colref.depth > 0) {
			return true;
		}
	}
	bool has_correlated_columns = false;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &child) {
		if (HasCorrelatedColumns(child)) {
			has_correlated_columns = true;
		}
	});
	return has_correlated_columns;
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::CreateJoin(JoinType type, unique_ptr<LogicalOperator> left_child,
                                                              unique_ptr<LogicalOperator> right_child,
                                                              unique_ptr<Expression> condition) {
	vector<JoinCondition> conditions;
	vector<unique_ptr<Expression>> arbitrary_expressions;
	LogicalComparisonJoin::ExtractJoinConditions(type, left_child, right_child, std::move(condition), conditions,
	                                             arbitrary_expressions);
	return LogicalComparisonJoin::CreateJoin(type, std::move(left_child), std::move(right_child), std::move(conditions),
	                                         std::move(arbitrary_expressions));
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundJoinRef &ref) {
	auto left = CreatePlan(*ref.left);
	auto right = CreatePlan(*ref.right);
	if (!ref.lateral && !ref.correlated_columns.empty()) {
		// non-lateral join with correlated columns
		// this happens if there is a join (or cross product) in a correlated subquery
		// due to the lateral binder the expression depth of all correlated columns in the "ref.correlated_columns" set
		// is 1 too high
		// we reduce expression depth of all columns in the "ref.correlated_columns" set by 1
		LateralBinder::ReduceExpressionDepth(*right, ref.correlated_columns);
	}
	if (ref.type == JoinType::RIGHT && ClientConfig::GetConfig(context).enable_optimizer) {
		// we turn any right outer joins into left outer joins for optimization purposes
		// they are the same but with sides flipped, so treating them the same simplifies life
		ref.type = JoinType::LEFT;
		std::swap(left, right);
	}
	if (ref.lateral) {
		// lateral join
		return PlanLateralJoin(std::move(left), std::move(right), ref.correlated_columns, ref.type,
		                       std::move(ref.condition));
	}
	if (ref.type == JoinType::INNER && (ref.condition->HasSubquery() || HasCorrelatedColumns(*ref.condition))) {
		// inner join, generate a cross product + filter
		// this will be later turned into a proper join by the join order optimizer
		auto root = LogicalCrossProduct::Create(std::move(left), std::move(right));

		auto filter = make_unique<LogicalFilter>(std::move(ref.condition));
		// visit the expressions in the filter
		for (auto &expression : filter->expressions) {
			PlanSubqueries(&expression, &root);
		}
		filter->AddChild(std::move(root));
		return std::move(filter);
	}

	// now create the join operator from the join condition
	auto result =
	    LogicalComparisonJoin::CreateJoin(ref.type, std::move(left), std::move(right), std::move(ref.condition));

	LogicalOperator *join;
	if (result->type == LogicalOperatorType::LOGICAL_FILTER) {
		join = result->children[0].get();
	} else {
		join = result.get();
	}
	for (auto &child : join->children) {
		if (child->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = (LogicalFilter &)*child;
			for (auto &expr : filter.expressions) {
				PlanSubqueries(&expr, &filter.children[0]);
			}
		}
	}

	// we visit the expressions depending on the type of join
	if (join->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		// comparison join
		// in this join we visit the expressions on the LHS with the LHS as root node
		// and the expressions on the RHS with the RHS as root node
		auto &comp_join = (LogicalComparisonJoin &)*join;
		for (idx_t i = 0; i < comp_join.conditions.size(); i++) {
			PlanSubqueries(&comp_join.conditions[i].left, &comp_join.children[0]);
			PlanSubqueries(&comp_join.conditions[i].right, &comp_join.children[1]);
		}
	} else if (join->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		auto &any_join = (LogicalAnyJoin &)*join;
		// for the any join we just visit the condition
		if (any_join.condition->HasSubquery()) {
			throw NotImplementedException("Cannot perform non-inner join on subquery!");
		}
	}
	return result;
}

} // namespace duckdb
