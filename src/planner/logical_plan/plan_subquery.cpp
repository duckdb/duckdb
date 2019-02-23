#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/list.hpp"
#include "parser/query_node/list.hpp"
#include "parser/statement/list.hpp"
#include "parser/tableref/list.hpp"
#include "planner/binder.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"
#include "planner/subquery/flatten_dependent_join.hpp"

#include <algorithm>
#include <map>

using namespace duckdb;
using namespace std;

static unique_ptr<Expression> PlanUncorrelatedSubquery(Binder &binder, BoundSubqueryExpression &expr,
                                                       SubqueryExpression &subquery, unique_ptr<LogicalOperator> &root,
                                                       unique_ptr<LogicalOperator> plan) {
	assert(!expr.IsCorrelated());
	switch (subquery.subquery_type) {
	case SubqueryType::EXISTS: {
		// uncorrelated EXISTS
		// we only care about existence, hence we push a LIMIT 1 operator
		auto limit = make_unique<LogicalLimit>(1, 0);
		limit->AddChild(move(plan));
		plan = move(limit);

		// now we push a COUNT(*) aggregate onto the limit, this will be either 0 or 1 (EXISTS or NOT EXISTS)
		auto count_star = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_COUNT_STAR, nullptr);
		count_star->ResolveType();
		auto count_type = count_star->return_type;
		vector<unique_ptr<Expression>> aggregate_list;
		aggregate_list.push_back(move(count_star));
		auto aggregate_index = binder.GenerateTableIndex();
		auto aggregate =
		    make_unique<LogicalAggregate>(binder.GenerateTableIndex(), aggregate_index, move(aggregate_list));
		aggregate->AddChild(move(plan));
		plan = move(aggregate);

		// now we push a projection with a comparison to 1
		auto left_child = make_unique<BoundColumnRefExpression>("", count_type, ColumnBinding(aggregate_index, 0));
		auto right_child = make_unique<ConstantExpression>(Value::Numeric(count_type, 1));
		auto comparison =
		    make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(left_child), move(right_child));

		vector<unique_ptr<Expression>> projection_list;
		projection_list.push_back(move(comparison));
		auto projection_index = binder.GenerateTableIndex();
		auto projection = make_unique<LogicalProjection>(projection_index, move(projection_list));
		projection->AddChild(move(plan));
		plan = move(projection);

		// we add it to the main query by adding a cross product
		// FIXME: should use something else besides cross product as we always add only one scalar constant
		auto cross_product = make_unique<LogicalCrossProduct>();
		cross_product->AddChild(move(root));
		cross_product->AddChild(move(plan));
		root = move(cross_product);

		// we replace the original subquery with a ColumnRefExpression refering to the result of the projection (either
		// TRUE or FALSE)
		return make_unique<BoundColumnRefExpression>(expr, TypeId::BOOLEAN, ColumnBinding(projection_index, 0));
	}
	case SubqueryType::SCALAR: {
		// in the uncorrelated case we are only interested in the first result of the query
		// hence we simply push a LIMIT 1 to get the first row of the subquery
		auto limit = make_unique<LogicalLimit>(1, 0);
		limit->AddChild(move(plan));
		plan = move(limit);
		// we push an aggregate that returns the FIRST element
		vector<unique_ptr<Expression>> expressions;
		auto bound = make_unique<BoundExpression>(expr.return_type, 0);
		auto first_agg = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_FIRST, move(bound));
		first_agg->ResolveType();
		expressions.push_back(move(first_agg));
		auto aggr_index = binder.GenerateTableIndex();
		auto aggr = make_unique<LogicalAggregate>(binder.GenerateTableIndex(), aggr_index, move(expressions));
		aggr->AddChild(move(plan));
		plan = move(aggr);

		// in the uncorrelated case, we add the value to the main query through a cross product
		// FIXME: should use something else besides cross product as we always add only one scalar constant and cross
		// product is not optimized for this.
		assert(root);
		auto cross_product = make_unique<LogicalCrossProduct>();
		cross_product->AddChild(move(root));
		cross_product->AddChild(move(plan));
		root = move(cross_product);

		// we replace the original subquery with a BoundColumnRefExpression refering to the first result of the
		// aggregation
		return make_unique<BoundColumnRefExpression>(expr, expr.return_type, ColumnBinding(aggr_index, 0));
	}
	default: {
		assert(subquery.subquery_type == SubqueryType::ANY);
		// we generate a MARK join that results in either (TRUE, FALSE or NULL)
		// subquery has NULL values -> result is (TRUE or NULL)
		// subquery has no NULL values -> result is (TRUE, FALSE or NULL [if input is NULL])
		// first we push a subquery to the right hand side
		plan->ResolveOperatorTypes();
		assert(plan->types.size() == 1);
		auto right_type = plan->types[0];
		auto return_type = max(subquery.child->return_type, right_type);

		auto subquery_index = binder.GenerateTableIndex();
		auto logical_subquery = make_unique<LogicalSubquery>(subquery_index, 1);
		logical_subquery->AddChild(move(plan));
		plan = move(logical_subquery);

		// then we generate the MARK join with the subquery
		auto join = make_unique<LogicalJoin>(JoinType::MARK);
		join->AddChild(move(root));
		join->AddChild(move(plan));
		// create the JOIN condition
		JoinCondition cond;
		cond.left = CastExpression::AddCastToType(return_type, move(subquery.child));
		cond.right = CastExpression::AddCastToType(return_type, make_unique<BoundColumnRefExpression>("", right_type, ColumnBinding(subquery_index, 0)));
		cond.comparison = subquery.comparison_type;
		join->conditions.push_back(move(cond));
		root = move(join);

		// we replace the original subquery with a BoundColumnRefExpression refering to the mark column
		return make_unique<BoundColumnRefExpression>(expr, expr.return_type, ColumnBinding(subquery_index, 0));
	}
	}
}

static unique_ptr<LogicalJoin> CreateDuplicateEliminatedJoin(vector<CorrelatedColumnInfo> &correlated_columns,
                                                             JoinType join_type) {
	auto delim_join = make_unique<LogicalDelimJoin>(join_type);
	for (size_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		delim_join->duplicate_eliminated_columns.push_back(
		    make_unique<BoundColumnRefExpression>("", col.type, col.binding));
	}
	return delim_join;
}

static void CreateDelimJoinConditions(LogicalJoin &delim_join, vector<CorrelatedColumnInfo> &correlated_columns,
                                      ColumnBinding base_binding) {
	for (size_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		JoinCondition cond;
		cond.left = make_unique<BoundColumnRefExpression>(col.name, col.type, col.binding);
		cond.right = make_unique<BoundColumnRefExpression>(
		    col.name, col.type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
		cond.comparison = ExpressionType::COMPARE_EQUAL;
		cond.null_values_are_equal = true;
		delim_join.conditions.push_back(move(cond));
	}
}

static unique_ptr<Expression> PlanCorrelatedSubquery(Binder &binder, BoundSubqueryExpression &expr,
                                                     SubqueryExpression &subquery, unique_ptr<LogicalOperator> &root,
                                                     unique_ptr<LogicalOperator> plan) {
	auto &correlated_columns = expr.binder->correlated_columns;
	assert(expr.IsCorrelated());
	// correlated subquery
	// for a more in-depth explanation of this code, read the paper "Unnesting Arbitrary Subqueries"
	// we handle three types of correlated subqueries: Scalar, EXISTS and ANY
	// all three cases are very similar with some minor changes (mainly the type of join performed at the end)
	switch (subquery.subquery_type) {
	case SubqueryType::SCALAR: {
		// correlated SCALAR query
		// first push a DUPLICATE ELIMINATED join
		// a duplicate eliminated join creates a duplicate eliminated copy of the LHS
		// and pushes it into any DUPLICATE_ELIMINATED SCAN operators on the RHS

		// in the SCALAR case, we create a SINGLE join (because we are only interested in obtaining the value)
		// NULL values are equal in this join because we join on the correlated columns ONLY
		// and e.g. in the query: SELECT (SELECT 42 FROM integers WHERE i1.i IS NULL LIMIT 1) FROM integers i1;
		// the input value NULL will generate the value 42, and we need to join NULL on the LHS with NULL on the RHS
		auto delim_join = CreateDuplicateEliminatedJoin(correlated_columns, JoinType::SINGLE);

		// the left side is the original plan
		// this is the side that will be duplicate eliminated and pushed into the RHS
		delim_join->AddChild(move(root));
		// the right side initially is a DEPENDENT join between the duplicate eliminated scan and the subquery
		// HOWEVER: we do not explicitly create the dependent join
		// instead, we eliminate the dependent join by pushing it down into the right side of the plan
		FlattenDependentJoins flatten(binder, correlated_columns);

		// first we check which logical operators have correlated expressions in the first place
		flatten.DetectCorrelatedExpressions(plan.get());
		// now we push the dependent join down
		auto dependent_join = flatten.PushDownDependentJoin(move(plan));

		// now the dependent join is fully eliminated
		// we only need to create the join conditions between the LHS and the RHS
		// first push a subquery node
		auto subquery_index = binder.GenerateTableIndex();
		auto subquery = make_unique<LogicalSubquery>(subquery_index, correlated_columns.size() + 1);
		subquery->AddChild(move(dependent_join));
		// now create the join conditions
		CreateDelimJoinConditions(*delim_join, correlated_columns, ColumnBinding(subquery_index, flatten.delim_offset));
		delim_join->AddChild(move(subquery));
		root = move(delim_join);
		// finally push the BoundColumnRefExpression referring to the data element returned by the join
		return make_unique<BoundColumnRefExpression>(expr, expr.return_type,
		                                             ColumnBinding(subquery_index, flatten.data_offset));
	}
	case SubqueryType::EXISTS: {
		// correlated EXISTS query
		// this query is similar to the correlated SCALAR query
		auto delim_join = CreateDuplicateEliminatedJoin(correlated_columns, JoinType::SINGLE);
		// LHS
		delim_join->AddChild(move(root));
		// RHS
		FlattenDependentJoins flatten(binder, correlated_columns);
		flatten.DetectCorrelatedExpressions(plan.get());
		auto dependent_join = flatten.PushDownDependentJoin(move(plan));

		// in the correlated EXISTS case we push a COUNT(*) aggregation that groups by the correlated columns
		// this gives us either (1) a count of how many entries occurred or (2) a NULL value if no entries occurred
		// COUNT(*)
		auto count_star = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_COUNT_STAR, nullptr);
		count_star->ResolveType();
		auto count_star_type = count_star->return_type;
		vector<unique_ptr<Expression>> aggregates;
		aggregates.push_back(move(count_star));
		// create the aggregate
		auto group_index = binder.GenerateTableIndex();
		auto aggr_index = binder.GenerateTableIndex();
		auto count_aggregate = make_unique<LogicalAggregate>(group_index, aggr_index, move(aggregates));
		// push the grouping columns
		for (size_t i = 0; i < correlated_columns.size(); i++) {
			auto &col = correlated_columns[i];
			count_aggregate->groups.push_back(make_unique<BoundColumnRefExpression>(
			    col.name, col.type,
			    ColumnBinding(flatten.base_binding.table_index, flatten.base_binding.column_index + i)));
		}
		count_aggregate->AddChild(move(dependent_join));

		// now we create the join conditions between the dependent join and the grouping columns
		CreateDelimJoinConditions(*delim_join, correlated_columns, ColumnBinding(group_index, 0));
		delim_join->AddChild(move(count_aggregate));
		root = move(delim_join);
		// finally we push the expression
		// EXISTS is TRUE if COUNT(*) IS NOT NULL
		// because the hash table will contain the value NULL if no values for the group are found
		// hence we push "COUNT(*) IS NOT NULL" instead of the original subquery expression
		auto bound_count = make_unique<BoundColumnRefExpression>("", count_star_type, ColumnBinding(aggr_index, 0));
		return make_unique<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, TypeId::BOOLEAN,
		                                       move(bound_count));
	}
	default: {
		assert(subquery.subquery_type == SubqueryType::ANY);
		// correlated ANY query
		// this query is similar to the correlated SCALAR query
		// however, in this case we push a correlated MARK join
		// note that in this join null values are NOT equal for ALL columns, but ONLY for the correlated columns
		// the correlated mark join handles this case by itself
		// as the MARK join has one extra join condition (the original condition, of the ANY expression, e.g.
		// [i=ANY(...)])
		plan->ResolveOperatorTypes();
		assert(plan->types.size() == 1);
		auto right_type = plan->types[0];
		auto return_type = max(subquery.child->return_type, right_type);

		auto delim_join = CreateDuplicateEliminatedJoin(correlated_columns, JoinType::MARK);
		// LHS
		delim_join->AddChild(move(root));
		// RHS
		FlattenDependentJoins flatten(binder, correlated_columns);
		flatten.DetectCorrelatedExpressions(plan.get());
		auto dependent_join = flatten.PushDownDependentJoin(move(plan));

		// push a subquery node under the duplicate eliminated join
		auto subquery_index = binder.GenerateTableIndex();
		auto subquery_node = make_unique<LogicalSubquery>(subquery_index, correlated_columns.size() + 1);
		subquery_node->AddChild(move(dependent_join));
		// now we create the join conditions between the dependent join and the original table
		CreateDelimJoinConditions(*delim_join, correlated_columns, ColumnBinding(subquery_index, flatten.delim_offset));
		// add the actual condition based on the ANY/ALL predicate
		JoinCondition compare_cond;
		compare_cond.left = CastExpression::AddCastToType(return_type, move(subquery.child));
		compare_cond.right = CastExpression::AddCastToType(return_type, make_unique<BoundColumnRefExpression>("", right_type, ColumnBinding(subquery_index, 0)));
		compare_cond.comparison = subquery.comparison_type;
		delim_join->conditions.push_back(move(compare_cond));

		delim_join->AddChild(move(subquery_node));
		root = move(delim_join);
		// finally push the BoundColumnRefExpression referring to the data element
		return make_unique<BoundColumnRefExpression>(expr, expr.return_type,
		                                             ColumnBinding(subquery_index, flatten.data_offset));
	}
	}
}

static unique_ptr<Expression> PlanSubquery(Binder &binder, ClientContext &context, BoundSubqueryExpression &expr,
                                           unique_ptr<LogicalOperator> &root);

class PlanSubqueries : public LogicalOperatorVisitor {
public:
	PlanSubqueries(Binder &binder, ClientContext &context) : binder(binder), context(context) {
	}
	void VisitOperator(LogicalOperator &op) override {
		if (op.children.size() > 0) {
			root = move(op.children[0]);
			VisitOperatorExpressions(op);
			op.children[0] = move(root);
			for (size_t i = 0; i < op.children.size(); i++) {
				VisitOperator(*op.children[i]);
			}
		}
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		return PlanSubquery(binder, context, expr, root);
	}

private:
	unique_ptr<LogicalOperator> root;
	Binder &binder;
	ClientContext &context;
};

static unique_ptr<Expression> PlanSubquery(Binder &binder, ClientContext &context, BoundSubqueryExpression &expr,
                                           unique_ptr<LogicalOperator> &root) {
	auto &subquery = (SubqueryExpression &)*expr.subquery;
	// first we translate the QueryNode of the subquery into a logical plan
	// note that we do not plan nested subqueries yet
	LogicalPlanGenerator generator(*expr.binder, context);
	generator.plan_subquery = false;
	generator.CreatePlan(*subquery.subquery);
	if (!generator.root) {
		throw Exception("Can't plan subquery");
	}
	if (!root) {
		throw Exception("Subquery cannot be root of a plan");
	}
	// now we actually flatten the subquery
	auto plan = move(generator.root);
	unique_ptr<Expression> result_expression;
	if (!expr.IsCorrelated()) {
		result_expression = PlanUncorrelatedSubquery(binder, expr, subquery, root, move(plan));
	} else {
		result_expression = PlanCorrelatedSubquery(binder, expr, subquery, root, move(plan));
	}
	// finally, we recursively plan the nested subqueries (if there are any)
	if (generator.has_unplanned_subqueries) {
		PlanSubqueries plan(binder, context);
		plan.VisitOperator(*root);
	}
	return result_expression;
}

unique_ptr<Expression> LogicalPlanGenerator::VisitReplace(BoundSubqueryExpression &expr,
                                                          unique_ptr<Expression> *expr_ptr) {
	// first visit the children of the Subquery expression, if any
	VisitExpressionChildren(expr);
	if (expr.IsCorrelated() && !plan_subquery) {
		// detected a nested correlated subquery
		// we don't plan it yet here, we are currently planning a subquery
		// nested subqueries will only be planned AFTER the current subquery has been flattened entirely
		has_unplanned_subqueries = true;
		return nullptr;
	}
	return PlanSubquery(binder, context, expr, root);
}
