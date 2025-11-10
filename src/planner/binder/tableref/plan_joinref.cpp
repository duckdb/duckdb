#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder/lateral_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_positional_join.hpp"
#include "duckdb/planner/subquery/recursive_dependent_join_planner.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"

namespace duckdb {

//! Only use conditions that are valid for the join ref type
static bool IsJoinTypeCondition(const JoinRefType ref_type, const ExpressionType expr_type) {
	switch (ref_type) {
	case JoinRefType::ASOF:
		switch (expr_type) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_LESSTHAN:
			return true;
		default:
			return false;
		}
	default:
		return true;
	}
}

//! Create a JoinCondition from a comparison
static bool CreateJoinCondition(Expression &expr, const unordered_set<idx_t> &left_bindings,
                                const unordered_set<idx_t> &right_bindings, vector<JoinCondition> &conditions) {
	// comparison
	auto &comparison = expr.Cast<BoundComparisonExpression>();
	auto left_side = JoinSide::GetJoinSide(*comparison.left, left_bindings, right_bindings);
	auto right_side = JoinSide::GetJoinSide(*comparison.right, left_bindings, right_bindings);
	if (left_side != JoinSide::BOTH && right_side != JoinSide::BOTH) {
		// join condition can be divided in a left/right side
		JoinCondition condition;
		condition.comparison = expr.GetExpressionType();
		auto left = std::move(comparison.left);
		auto right = std::move(comparison.right);
		if (left_side == JoinSide::RIGHT) {
			// left = right, right = left, flip the comparison symbol and reverse sides
			swap(left, right);
			condition.comparison = FlipComparisonExpression(expr.GetExpressionType());
		}
		condition.left = std::move(left);
		condition.right = std::move(right);
		conditions.push_back(std::move(condition));
		return true;
	}
	return false;
}

void LogicalComparisonJoin::ExtractJoinConditions(
    ClientContext &context, JoinType type, JoinRefType ref_type, unique_ptr<LogicalOperator> &left_child,
    unique_ptr<LogicalOperator> &right_child, const unordered_set<idx_t> &left_bindings,
    const unordered_set<idx_t> &right_bindings, vector<unique_ptr<Expression>> &expressions,
    vector<JoinCondition> &conditions, vector<unique_ptr<Expression>> &arbitrary_expressions) {
	for (auto &expr : expressions) {
		auto total_side = JoinSide::GetJoinSide(*expr, left_bindings, right_bindings);
		if (total_side != JoinSide::BOTH) {
			// join condition does not reference both sides, add it as filter under the join
			if ((type == JoinType::LEFT || ref_type == JoinRefType::ASOF) && total_side == JoinSide::RIGHT) {
				// filter is on RHS and the join is a LEFT OUTER join, we can push it in the right child
				if (right_child->type != LogicalOperatorType::LOGICAL_FILTER) {
					// not a filter yet, push a new empty filter
					auto filter = make_uniq<LogicalFilter>();
					filter->AddChild(std::move(right_child));
					right_child = std::move(filter);
				}
				// push the expression into the filter
				auto &filter = right_child->Cast<LogicalFilter>();
				filter.expressions.push_back(std::move(expr));
				continue;
			}
			// if the join is a LEFT JOIN and the join expression constantly evaluates to TRUE,
			// then we do not add it to the arbitrary expressions
			if (type == JoinType::LEFT && expr->IsFoldable()) {
				Value result;
				ExpressionExecutor::TryEvaluateScalar(context, *expr, result);
				if (!result.IsNull() && result == Value(true)) {
					continue;
				}
			}
		} else if (expr->GetExpressionType() == ExpressionType::COMPARE_EQUAL ||
		           expr->GetExpressionType() == ExpressionType::COMPARE_NOTEQUAL ||
		           expr->GetExpressionType() == ExpressionType::COMPARE_BOUNDARY_START ||
		           expr->GetExpressionType() == ExpressionType::COMPARE_LESSTHAN ||
		           expr->GetExpressionType() == ExpressionType::COMPARE_GREATERTHAN ||
		           expr->GetExpressionType() == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
		           expr->GetExpressionType() == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
		           expr->GetExpressionType() == ExpressionType::COMPARE_BOUNDARY_START ||
		           expr->GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM ||
		           expr->GetExpressionType() == ExpressionType::COMPARE_DISTINCT_FROM)

		{
			// comparison, check if we can create a comparison JoinCondition
			if (IsJoinTypeCondition(ref_type, expr->GetExpressionType()) &&
			    CreateJoinCondition(*expr, left_bindings, right_bindings, conditions)) {
				// successfully created the join condition
				continue;
			}
		}
		arbitrary_expressions.push_back(std::move(expr));
	}
}

void LogicalComparisonJoin::ExtractJoinConditions(ClientContext &context, JoinType type, JoinRefType ref_type,
                                                  unique_ptr<LogicalOperator> &left_child,
                                                  unique_ptr<LogicalOperator> &right_child,
                                                  vector<unique_ptr<Expression>> &expressions,
                                                  vector<JoinCondition> &conditions,
                                                  vector<unique_ptr<Expression>> &arbitrary_expressions) {
	unordered_set<idx_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*left_child, left_bindings);
	LogicalJoin::GetTableReferences(*right_child, right_bindings);
	return ExtractJoinConditions(context, type, ref_type, left_child, right_child, left_bindings, right_bindings,
	                             expressions, conditions, arbitrary_expressions);
}

void LogicalComparisonJoin::ExtractJoinConditions(ClientContext &context, JoinType type, JoinRefType ref_type,
                                                  unique_ptr<LogicalOperator> &left_child,
                                                  unique_ptr<LogicalOperator> &right_child,
                                                  unique_ptr<Expression> condition, vector<JoinCondition> &conditions,
                                                  vector<unique_ptr<Expression>> &arbitrary_expressions) {
	// split the expressions by the AND clause
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(std::move(condition));
	LogicalFilter::SplitPredicates(expressions);
	return ExtractJoinConditions(context, type, ref_type, left_child, right_child, expressions, conditions,
	                             arbitrary_expressions);
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::CreateJoin(ClientContext &context, JoinType type,
                                                              JoinRefType reftype,
                                                              unique_ptr<LogicalOperator> left_child,
                                                              unique_ptr<LogicalOperator> right_child,
                                                              vector<JoinCondition> conditions,
                                                              vector<unique_ptr<Expression>> arbitrary_expressions) {
	// Validate the conditions
	bool need_to_consider_arbitrary_expressions = true;
	const bool is_asof = reftype == JoinRefType::ASOF;
	if (is_asof) {
		// Handle case of zero conditions
		auto asof_idx = conditions.size() + 1;
		for (size_t c = 0; c < conditions.size(); ++c) {
			auto &cond = conditions[c];
			switch (cond.comparison) {
			case ExpressionType::COMPARE_EQUAL:
			case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			case ExpressionType::COMPARE_LESSTHAN:
				if (asof_idx < conditions.size()) {
					throw BinderException("Multiple ASOF JOIN inequalities");
				}
				asof_idx = c;
				break;
			default:
				throw BinderException("Invalid ASOF JOIN comparison");
			}
		}
		if (asof_idx >= conditions.size()) {
			throw BinderException("Missing ASOF JOIN inequality");
		}
	}

	if (type == JoinType::INNER && reftype == JoinRefType::REGULAR) {
		// for inner joins we can push arbitrary expressions as a filter
		// here we prefer to create a comparison join if possible
		// that way we can use the much faster hash join to process the main join
		// rather than doing a nested loop join to handle arbitrary expressions

		// for left and full outer joins we HAVE to process all join conditions
		// because pushing a filter will lead to an incorrect result, as non-matching tuples cannot be filtered out
		need_to_consider_arbitrary_expressions = false;
	}
	if ((need_to_consider_arbitrary_expressions && !arbitrary_expressions.empty()) || conditions.empty()) {
		if (is_asof) {
			D_ASSERT(!conditions.empty());
			// We still need to produce an ASOF join here, but it will have to evaluate the arbitrary conditions itself
			auto asof_join = make_uniq<LogicalComparisonJoin>(type, LogicalOperatorType::LOGICAL_ASOF_JOIN);
			asof_join->conditions = std::move(conditions);
			asof_join->children.push_back(std::move(left_child));
			asof_join->children.push_back(std::move(right_child));
			// AND all the arbitrary expressions together
			asof_join->predicate = std::move(arbitrary_expressions[0]);
			for (idx_t i = 1; i < arbitrary_expressions.size(); i++) {
				asof_join->predicate = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
				                                                             std::move(asof_join->predicate),
				                                                             std::move(arbitrary_expressions[i]));
			}
			return std::move(asof_join);
		}

		if (arbitrary_expressions.empty()) {
			// all conditions were pushed down, add TRUE predicate
			arbitrary_expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
		}
		// if we get here we could not create any JoinConditions
		// turn this into an arbitrary expression join
		auto any_join = make_uniq<LogicalAnyJoin>(type);
		// create the condition
		any_join->children.push_back(std::move(left_child));
		any_join->children.push_back(std::move(right_child));
		// AND all the arbitrary expressions together
		// do the same with any remaining conditions
		idx_t start_idx = 0;
		if (conditions.empty()) {
			// no conditions, just use the arbitrary expressions
			any_join->condition = std::move(arbitrary_expressions[0]);
			start_idx = 1;
		} else {
			// we have some conditions as well
			any_join->condition = JoinCondition::CreateExpression(std::move(conditions[0]));
			for (idx_t i = 1; i < conditions.size(); i++) {
				any_join->condition = make_uniq<BoundConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(any_join->condition),
				    JoinCondition::CreateExpression(std::move(conditions[i])));
			}
		}
		for (idx_t i = start_idx; i < arbitrary_expressions.size(); i++) {
			any_join->condition = make_uniq<BoundConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, std::move(any_join->condition), std::move(arbitrary_expressions[i]));
		}
		return std::move(any_join);
	} else {
		// we successfully converted expressions into JoinConditions
		// create a LogicalComparisonJoin
		auto logical_type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
		if (is_asof) {
			logical_type = LogicalOperatorType::LOGICAL_ASOF_JOIN;
		}
		auto comp_join = make_uniq<LogicalComparisonJoin>(type, logical_type);
		comp_join->conditions = std::move(conditions);
		comp_join->children.push_back(std::move(left_child));
		comp_join->children.push_back(std::move(right_child));
		if (!arbitrary_expressions.empty()) {
			// we have some arbitrary expressions as well
			// add them to a filter
			auto filter = make_uniq<LogicalFilter>();
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

static bool HasCorrelatedColumns(const Expression &root_expr) {
	bool has_correlated_columns = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(root_expr,
	                                                              [&](const BoundColumnRefExpression &colref) {
		                                                              if (colref.depth > 0) {
			                                                              has_correlated_columns = true;
		                                                              }
	                                                              });
	return has_correlated_columns;
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::CreateJoin(ClientContext &context, JoinType type,
                                                              JoinRefType reftype,
                                                              unique_ptr<LogicalOperator> left_child,
                                                              unique_ptr<LogicalOperator> right_child,
                                                              unique_ptr<Expression> condition) {
	vector<JoinCondition> conditions;
	vector<unique_ptr<Expression>> arbitrary_expressions;
	LogicalComparisonJoin::ExtractJoinConditions(context, type, reftype, left_child, right_child, std::move(condition),
	                                             conditions, arbitrary_expressions);
	return LogicalComparisonJoin::CreateJoin(context, type, reftype, std::move(left_child), std::move(right_child),
	                                         std::move(conditions), std::move(arbitrary_expressions));
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundJoinRef &ref) {
	auto old_is_outside_flattened = is_outside_flattened;
	// Plan laterals from outermost to innermost
	if (ref.lateral) {
		// Set the flag to ensure that children do not flatten before the root
		is_outside_flattened = false;
	}
	auto left = std::move(ref.left.plan);
	auto right = std::move(ref.right.plan);
	is_outside_flattened = old_is_outside_flattened;

	// For joins, depth of the bindings will be one higher on the right because of the lateral binder
	// If the current join does not have correlations between left and right, then the right bindings
	// have depth 1 too high and can be reduced by 1 throughout
	if (!ref.lateral && !ref.correlated_columns.empty()) {
		LateralBinder::ReduceExpressionDepth(*right, ref.correlated_columns);
	}

	if (ref.type == JoinType::RIGHT && ref.ref_type != JoinRefType::ASOF &&
	    ClientConfig::GetConfig(context).enable_optimizer &&
	    !Optimizer::OptimizerDisabled(context, OptimizerType::BUILD_SIDE_PROBE_SIDE)) {
		// we turn any right outer joins into left outer joins for optimization purposes
		// they are the same but with sides flipped, so treating them the same simplifies life
		ref.type = JoinType::LEFT;
		std::swap(left, right);
	}
	if (ref.lateral) {
		auto new_plan = PlanLateralJoin(std::move(left), std::move(right), ref.correlated_columns, ref.type,
		                                std::move(ref.condition));
		if (has_unplanned_dependent_joins) {
			RecursiveDependentJoinPlanner plan(*this);
			plan.VisitOperator(*new_plan);
		}
		return new_plan;
	}
	switch (ref.ref_type) {
	case JoinRefType::CROSS:
		return LogicalCrossProduct::Create(std::move(left), std::move(right));
	case JoinRefType::POSITIONAL:
		return LogicalPositionalJoin::Create(std::move(left), std::move(right));
	default:
		break;
	}
	if (ref.type == JoinType::INNER && (ref.condition->HasSubquery() || HasCorrelatedColumns(*ref.condition)) &&
	    ref.ref_type == JoinRefType::REGULAR) {
		// inner join, generate a cross product + filter
		// this will be later turned into a proper join by the join order optimizer
		auto root = LogicalCrossProduct::Create(std::move(left), std::move(right));

		auto filter = make_uniq<LogicalFilter>(std::move(ref.condition));
		// visit the expressions in the filter
		for (auto &expression : filter->expressions) {
			PlanSubqueries(expression, root);
		}
		filter->AddChild(std::move(root));
		return std::move(filter);
	}

	// now create the join operator from the join condition
	auto result = LogicalComparisonJoin::CreateJoin(context, ref.type, ref.ref_type, std::move(left), std::move(right),
	                                                std::move(ref.condition));
	optional_ptr<LogicalOperator> join;
	if (result->type == LogicalOperatorType::LOGICAL_FILTER) {
		join = result->children[0].get();
	} else {
		join = result.get();
	}

	if (ref.type == JoinType::MARK) {
		join->Cast<LogicalJoin>().mark_index = ref.mark_index;
	}
	for (auto &child : join->children) {
		if (child->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = child->Cast<LogicalFilter>();
			for (auto &expr : filter.expressions) {
				PlanSubqueries(expr, filter.children[0]);
			}
		}
	}

	// we visit the expressions depending on the type of join
	switch (join->type) {
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		// comparison join
		// in this join we visit the expressions on the LHS with the LHS as root node
		// and the expressions on the RHS with the RHS as root node
		auto &comp_join = join->Cast<LogicalComparisonJoin>();
		for (idx_t i = 0; i < comp_join.conditions.size(); i++) {
			PlanSubqueries(comp_join.conditions[i].left, comp_join.children[0]);
			PlanSubqueries(comp_join.conditions[i].right, comp_join.children[1]);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &any_join = join->Cast<LogicalAnyJoin>();
		// for the any join we just visit the condition
		if (any_join.condition->HasSubquery()) {
			throw NotImplementedException("Cannot perform non-inner join on subquery!");
		}
		break;
	}
	default:
		break;
	}
	if (!ref.duplicate_eliminated_columns.empty()) {
		auto &comp_join = join->Cast<LogicalComparisonJoin>();
		comp_join.type = LogicalOperatorType::LOGICAL_DELIM_JOIN;
		comp_join.delim_flipped = ref.delim_flipped;
		for (auto &col : ref.duplicate_eliminated_columns) {
			comp_join.duplicate_eliminated_columns.emplace_back(col->Copy());
		}
	}
	return result;
}

} // namespace duckdb
