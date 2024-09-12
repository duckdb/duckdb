#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/subquery/flatten_dependent_join.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/subquery/recursive_dependent_join_planner.hpp"
#include "duckdb/core_functions/scalar/generic_functions.hpp"

namespace duckdb {

static unique_ptr<Expression> PlanUncorrelatedSubquery(Binder &binder, BoundSubqueryExpression &expr,
                                                       unique_ptr<LogicalOperator> &root,
                                                       unique_ptr<LogicalOperator> plan) {
	D_ASSERT(!expr.IsCorrelated());
	switch (expr.subquery_type) {
	case SubqueryType::EXISTS: {
		// uncorrelated EXISTS
		// we only care about existence, hence we push a LIMIT 1 operator
		auto limit = make_uniq<LogicalLimit>(BoundLimitNode::ConstantValue(1), BoundLimitNode());
		limit->AddChild(std::move(plan));
		plan = std::move(limit);

		// now we push a COUNT(*) aggregate onto the limit, this will be either 0 or 1 (EXISTS or NOT EXISTS)
		auto count_star_fun = CountStarFun::GetFunction();

		FunctionBinder function_binder(binder.context);
		auto count_star =
		    function_binder.BindAggregateFunction(count_star_fun, {}, nullptr, AggregateType::NON_DISTINCT);
		auto idx_type = count_star->return_type;
		vector<unique_ptr<Expression>> aggregate_list;
		aggregate_list.push_back(std::move(count_star));
		auto aggregate_index = binder.GenerateTableIndex();
		auto aggregate =
		    make_uniq<LogicalAggregate>(binder.GenerateTableIndex(), aggregate_index, std::move(aggregate_list));
		aggregate->AddChild(std::move(plan));
		plan = std::move(aggregate);

		// now we push a projection with a comparison to 1
		auto left_child = make_uniq<BoundColumnRefExpression>(idx_type, ColumnBinding(aggregate_index, 0));
		auto right_child = make_uniq<BoundConstantExpression>(Value::Numeric(idx_type, 1));
		auto comparison = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(left_child),
		                                                       std::move(right_child));

		vector<unique_ptr<Expression>> projection_list;
		projection_list.push_back(std::move(comparison));
		auto projection_index = binder.GenerateTableIndex();
		auto projection = make_uniq<LogicalProjection>(projection_index, std::move(projection_list));
		projection->AddChild(std::move(plan));
		plan = std::move(projection);

		// we add it to the main query by adding a cross product
		// FIXME: should use something else besides cross product as we always add only one scalar constant
		root = LogicalCrossProduct::Create(std::move(root), std::move(plan));

		// we replace the original subquery with a ColumnRefExpression referring to the result of the projection (either
		// TRUE or FALSE)
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), LogicalType::BOOLEAN,
		                                           ColumnBinding(projection_index, 0));
	}
	case SubqueryType::SCALAR: {
		// uncorrelated scalar, we want to return the first entry
		// figure out the table index of the bound table of the entry which we want to return
		auto bindings = plan->GetColumnBindings();
		D_ASSERT(bindings.size() == 1);
		idx_t table_idx = bindings[0].table_index;

		auto &config = ClientConfig::GetConfig(binder.context);
		bool error_on_multiple_rows = config.scalar_subquery_error_on_multiple_rows;

		// we push an aggregate that returns the FIRST element
		vector<unique_ptr<Expression>> expressions;
		auto bound = make_uniq<BoundColumnRefExpression>(expr.return_type, ColumnBinding(table_idx, 0));
		vector<unique_ptr<Expression>> first_children;
		first_children.push_back(std::move(bound));

		FunctionBinder function_binder(binder.context);
		auto first_agg = function_binder.BindAggregateFunction(
		    FirstFun::GetFunction(expr.return_type), std::move(first_children), nullptr, AggregateType::NON_DISTINCT);

		expressions.push_back(std::move(first_agg));
		if (error_on_multiple_rows) {
			vector<unique_ptr<Expression>> count_children;
			auto count_agg = function_binder.BindAggregateFunction(
			    CountStarFun::GetFunction(), std::move(count_children), nullptr, AggregateType::NON_DISTINCT);
			expressions.push_back(std::move(count_agg));
		}
		auto aggr_index = binder.GenerateTableIndex();

		auto aggr = make_uniq<LogicalAggregate>(binder.GenerateTableIndex(), aggr_index, std::move(expressions));
		aggr->AddChild(std::move(plan));
		plan = std::move(aggr);

		if (error_on_multiple_rows) {
			// CASE WHEN count > 1 THEN error('Scalar subquery can only return a single row') ELSE first_agg END
			idx_t proj_index = binder.GenerateTableIndex();

			auto first_ref =
			    make_uniq<BoundColumnRefExpression>(plan->expressions[0]->return_type, ColumnBinding(aggr_index, 0));
			auto count_ref =
			    make_uniq<BoundColumnRefExpression>(plan->expressions[1]->return_type, ColumnBinding(aggr_index, 1));

			auto constant_one = make_uniq<BoundConstantExpression>(Value::BIGINT(1));
			auto count_check = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN,
			                                                        std::move(count_ref), std::move(constant_one));

			vector<unique_ptr<Expression>> error_children;
			error_children.push_back(make_uniq<BoundConstantExpression>(
			    Value("More than one row returned by a subquery used as an expression - scalar subqueries can only "
			          "return a single row.\n\nUse \"SET scalar_subquery_error_on_multiple_rows=false\" to revert to "
			          "previous behavior of returning a random row.")));
			auto error_expr = function_binder.BindScalarFunction(ErrorFun::GetFunction(), std::move(error_children));
			error_expr->return_type = first_ref->return_type;
			auto case_expr =
			    make_uniq<BoundCaseExpression>(std::move(count_check), std::move(error_expr), std::move(first_ref));

			vector<unique_ptr<Expression>> proj_expressions;
			proj_expressions.push_back(std::move(case_expr));

			auto proj = make_uniq<LogicalProjection>(proj_index, std::move(proj_expressions));
			proj->AddChild(std::move(plan));
			plan = std::move(proj);

			aggr_index = proj_index;
		}

		// in the uncorrelated case, we add the value to the main query through a cross product
		// FIXME: should use something else besides cross product as we always add only one scalar constant and cross
		// product is not optimized for this.
		D_ASSERT(root);
		root = LogicalCrossProduct::Create(std::move(root), std::move(plan));

		// we replace the original subquery with a BoundColumnRefExpression referring to the first result of the
		// aggregation
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, ColumnBinding(aggr_index, 0));
	}
	default: {
		D_ASSERT(expr.subquery_type == SubqueryType::ANY);
		// we generate a MARK join that results in either (TRUE, FALSE or NULL)
		// subquery has NULL values -> result is (TRUE or NULL)
		// subquery has no NULL values -> result is (TRUE, FALSE or NULL [if input is NULL])
		// fetch the column bindings
		auto plan_columns = plan->GetColumnBindings();

		// then we generate the MARK join with the subquery
		idx_t mark_index = binder.GenerateTableIndex();
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::MARK);
		join->mark_index = mark_index;
		join->AddChild(std::move(root));
		join->AddChild(std::move(plan));
		// create the JOIN condition
		JoinCondition cond;
		cond.left = std::move(expr.child);
		cond.right = BoundCastExpression::AddDefaultCastToType(
		    make_uniq<BoundColumnRefExpression>(expr.child_type, plan_columns[0]), expr.child_target);
		cond.comparison = expr.comparison_type;
		join->conditions.push_back(std::move(cond));
		root = std::move(join);

		// we replace the original subquery with a BoundColumnRefExpression referring to the mark column
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, ColumnBinding(mark_index, 0));
	}
	}
}

static unique_ptr<LogicalComparisonJoin>
CreateDuplicateEliminatedJoin(const vector<CorrelatedColumnInfo> &correlated_columns, JoinType join_type,
                              unique_ptr<LogicalOperator> original_plan, bool perform_delim) {
	auto delim_join = make_uniq<LogicalComparisonJoin>(join_type, LogicalOperatorType::LOGICAL_DELIM_JOIN);
	if (!perform_delim) {
		// if we are not performing a delim join, we push a row_number() OVER() window operator on the LHS
		// and perform all duplicate elimination on that row number instead
		D_ASSERT(correlated_columns[0].type.id() == LogicalTypeId::BIGINT);
		auto window = make_uniq<LogicalWindow>(correlated_columns[0].binding.table_index);
		auto row_number =
		    make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_ROW_NUMBER, LogicalType::BIGINT, nullptr, nullptr);
		row_number->start = WindowBoundary::UNBOUNDED_PRECEDING;
		row_number->end = WindowBoundary::CURRENT_ROW_ROWS;
		row_number->alias = "delim_index";
		window->expressions.push_back(std::move(row_number));
		window->AddChild(std::move(original_plan));
		original_plan = std::move(window);
	}
	delim_join->AddChild(std::move(original_plan));
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		delim_join->duplicate_eliminated_columns.push_back(make_uniq<BoundColumnRefExpression>(col.type, col.binding));
		delim_join->mark_types.push_back(col.type);
	}
	return delim_join;
}

static void CreateDelimJoinConditions(LogicalComparisonJoin &delim_join,
                                      const vector<CorrelatedColumnInfo> &correlated_columns,
                                      vector<ColumnBinding> bindings, idx_t base_offset, bool perform_delim) {
	auto col_count = perform_delim ? correlated_columns.size() : 1;
	for (idx_t i = 0; i < col_count; i++) {
		auto &col = correlated_columns[i];
		auto binding_idx = base_offset + i;
		if (binding_idx >= bindings.size()) {
			throw InternalException("Delim join - binding index out of range");
		}
		JoinCondition cond;
		cond.left = make_uniq<BoundColumnRefExpression>(col.name, col.type, col.binding);
		cond.right = make_uniq<BoundColumnRefExpression>(col.name, col.type, bindings[binding_idx]);
		cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
		delim_join.conditions.push_back(std::move(cond));
	}
}

static bool PerformDelimOnType(const LogicalType &type) {
	if (type.InternalType() == PhysicalType::LIST) {
		return false;
	}
	if (type.InternalType() == PhysicalType::STRUCT) {
		for (auto &entry : StructType::GetChildTypes(type)) {
			if (!PerformDelimOnType(entry.second)) {
				return false;
			}
		}
	}
	return true;
}

static bool PerformDuplicateElimination(Binder &binder, vector<CorrelatedColumnInfo> &correlated_columns) {
	if (!ClientConfig::GetConfig(binder.context).enable_optimizer) {
		// if optimizations are disabled we always do a delim join
		return true;
	}
	bool perform_delim = true;
	for (auto &col : correlated_columns) {
		if (!PerformDelimOnType(col.type)) {
			perform_delim = false;
			break;
		}
	}
	if (perform_delim) {
		return true;
	}
	auto binding = ColumnBinding(binder.GenerateTableIndex(), 0);
	auto type = LogicalType::BIGINT;
	auto name = "delim_index";
	CorrelatedColumnInfo info(binding, type, name, 0);
	correlated_columns.insert(correlated_columns.begin(), std::move(info));
	return false;
}

static unique_ptr<Expression> PlanCorrelatedSubquery(Binder &binder, BoundSubqueryExpression &expr,
                                                     unique_ptr<LogicalOperator> &root,
                                                     unique_ptr<LogicalOperator> plan) {
	auto &correlated_columns = expr.binder->correlated_columns;
	// FIXME: there should be a way of disabling decorrelation for ANY queries as well, but not for now...
	bool perform_delim =
	    expr.subquery_type == SubqueryType::ANY ? true : PerformDuplicateElimination(binder, correlated_columns);
	D_ASSERT(expr.IsCorrelated());
	// correlated subquery
	// for a more in-depth explanation of this code, read the paper "Unnesting Arbitrary Subqueries"
	// we handle three types of correlated subqueries: Scalar, EXISTS and ANY
	// all three cases are very similar with some minor changes (mainly the type of join performed at the end)
	switch (expr.subquery_type) {
	case SubqueryType::SCALAR: {
		// correlated SCALAR query
		// first push a DUPLICATE ELIMINATED join
		// a duplicate eliminated join creates a duplicate eliminated copy of the LHS
		// and pushes it into any DUPLICATE_ELIMINATED SCAN operators on the RHS

		// in the SCALAR case, we create a SINGLE join (because we are only interested in obtaining the value)
		// NULL values are equal in this join because we join on the correlated columns ONLY
		// and e.g. in the query: SELECT (SELECT 42 FROM integers WHERE i1.i IS NULL LIMIT 1) FROM integers i1;
		// the input value NULL will generate the value 42, and we need to join NULL on the LHS with NULL on the RHS
		// the left side is the original plan
		// this is the side that will be duplicate eliminated and pushed into the RHS
		auto delim_join =
		    CreateDuplicateEliminatedJoin(correlated_columns, JoinType::SINGLE, std::move(root), perform_delim);

		// the right side initially is a DEPENDENT join between the duplicate eliminated scan and the subquery
		// HOWEVER: we do not explicitly create the dependent join
		// instead, we eliminate the dependent join by pushing it down into the right side of the plan
		FlattenDependentJoins flatten(binder, correlated_columns, perform_delim);

		// first we check which logical operators have correlated expressions in the first place
		flatten.DetectCorrelatedExpressions(*plan);
		// now we push the dependent join down
		auto dependent_join = flatten.PushDownDependentJoin(std::move(plan));

		// now the dependent join is fully eliminated
		// we only need to create the join conditions between the LHS and the RHS
		// fetch the set of columns
		auto plan_columns = dependent_join->GetColumnBindings();

		// now create the join conditions
		CreateDelimJoinConditions(*delim_join, correlated_columns, plan_columns, flatten.delim_offset, perform_delim);
		delim_join->AddChild(std::move(dependent_join));
		root = std::move(delim_join);
		// finally push the BoundColumnRefExpression referring to the data element returned by the join
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, plan_columns[flatten.data_offset]);
	}
	case SubqueryType::EXISTS: {
		// correlated EXISTS query
		// this query is similar to the correlated SCALAR query, except we use a MARK join here
		idx_t mark_index = binder.GenerateTableIndex();
		auto delim_join =
		    CreateDuplicateEliminatedJoin(correlated_columns, JoinType::MARK, std::move(root), perform_delim);
		delim_join->mark_index = mark_index;
		// RHS
		FlattenDependentJoins flatten(binder, correlated_columns, perform_delim, true);
		flatten.DetectCorrelatedExpressions(*plan);
		auto dependent_join = flatten.PushDownDependentJoin(std::move(plan));

		// fetch the set of columns
		auto plan_columns = dependent_join->GetColumnBindings();

		// now we create the join conditions between the dependent join and the original table
		CreateDelimJoinConditions(*delim_join, correlated_columns, plan_columns, flatten.delim_offset, perform_delim);
		delim_join->AddChild(std::move(dependent_join));
		root = std::move(delim_join);
		// finally push the BoundColumnRefExpression referring to the marker
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, ColumnBinding(mark_index, 0));
	}
	default: {
		D_ASSERT(expr.subquery_type == SubqueryType::ANY);
		// correlated ANY query
		// this query is similar to the correlated SCALAR query
		// however, in this case we push a correlated MARK join
		// note that in this join null values are NOT equal for ALL columns, but ONLY for the correlated columns
		// the correlated mark join handles this case by itself
		// as the MARK join has one extra join condition (the original condition, of the ANY expression, e.g.
		// [i=ANY(...)])
		idx_t mark_index = binder.GenerateTableIndex();
		auto delim_join =
		    CreateDuplicateEliminatedJoin(correlated_columns, JoinType::MARK, std::move(root), perform_delim);
		delim_join->mark_index = mark_index;
		// RHS
		FlattenDependentJoins flatten(binder, correlated_columns, true, true);
		flatten.DetectCorrelatedExpressions(*plan);
		auto dependent_join = flatten.PushDownDependentJoin(std::move(plan));

		// fetch the columns
		auto plan_columns = dependent_join->GetColumnBindings();

		// now we create the join conditions between the dependent join and the original table
		CreateDelimJoinConditions(*delim_join, correlated_columns, plan_columns, flatten.delim_offset, perform_delim);
		// add the actual condition based on the ANY/ALL predicate
		JoinCondition compare_cond;
		compare_cond.left = std::move(expr.child);
		compare_cond.right = BoundCastExpression::AddDefaultCastToType(
		    make_uniq<BoundColumnRefExpression>(expr.child_type, plan_columns[0]), expr.child_target);
		compare_cond.comparison = expr.comparison_type;
		delim_join->conditions.push_back(std::move(compare_cond));

		delim_join->AddChild(std::move(dependent_join));
		root = std::move(delim_join);
		// finally push the BoundColumnRefExpression referring to the marker
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, ColumnBinding(mark_index, 0));
	}
	}
}

void RecursiveDependentJoinPlanner::VisitOperator(LogicalOperator &op) {
	if (!op.children.empty()) {
		// Collect all recursive CTEs during recursive descend
		if (op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
			auto &rec_cte = op.Cast<LogicalRecursiveCTE>();
			binder.recursive_ctes[rec_cte.table_index] = &op;
		}
		root = std::move(op.children[0]);
		D_ASSERT(root);
		if (root->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
			// Found a dependent join, flatten it
			auto &new_root = root->Cast<LogicalDependentJoin>();
			root = binder.PlanLateralJoin(std::move(new_root.children[0]), std::move(new_root.children[1]),
			                              new_root.correlated_columns, new_root.join_type,
			                              std::move(new_root.join_condition));
		}
		VisitOperatorExpressions(op);
		op.children[0] = std::move(root);
		for (idx_t i = 0; i < op.children.size(); i++) {
			D_ASSERT(op.children[i]);
			VisitOperator(*op.children[i]);
		}
	}
}

unique_ptr<Expression> RecursiveDependentJoinPlanner::VisitReplace(BoundSubqueryExpression &expr,
                                                                   unique_ptr<Expression> *expr_ptr) {
	return binder.PlanSubquery(expr, root);
}

unique_ptr<Expression> Binder::PlanSubquery(BoundSubqueryExpression &expr, unique_ptr<LogicalOperator> &root) {
	D_ASSERT(root);
	// first we translate the QueryNode of the subquery into a logical plan
	// note that we do not plan nested subqueries yet
	auto sub_binder = Binder::CreateBinder(context, this);
	sub_binder->is_outside_flattened = false;
	auto subquery_root = sub_binder->CreatePlan(*expr.subquery);
	D_ASSERT(subquery_root);

	// now we actually flatten the subquery
	auto plan = std::move(subquery_root);

	unique_ptr<Expression> result_expression;
	if (!expr.IsCorrelated()) {
		result_expression = PlanUncorrelatedSubquery(*this, expr, root, std::move(plan));
	} else {
		result_expression = PlanCorrelatedSubquery(*this, expr, root, std::move(plan));
	}
	// finally, we recursively plan the nested subqueries (if there are any)
	if (sub_binder->has_unplanned_dependent_joins) {
		RecursiveDependentJoinPlanner plan(*this);
		plan.VisitOperator(*root);
	}
	return result_expression;
}

void Binder::PlanSubqueries(unique_ptr<Expression> &expr_ptr, unique_ptr<LogicalOperator> &root) {
	if (!expr_ptr) {
		return;
	}
	auto &expr = *expr_ptr;
	// first visit the children of the node, if any
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &expr) { PlanSubqueries(expr, root); });

	// check if this is a subquery node
	if (expr.expression_class == ExpressionClass::BOUND_SUBQUERY) {
		auto &subquery = expr.Cast<BoundSubqueryExpression>();
		// subquery node! plan it
		if (!is_outside_flattened) {
			// detected a nested correlated subquery
			// we don't plan it yet here, we are currently planning a subquery
			// nested subqueries will only be planned AFTER the current subquery has been flattened entirely
			has_unplanned_dependent_joins = true;
			return;
		}
		expr_ptr = PlanSubquery(subquery, root);
	}
}

unique_ptr<LogicalOperator> Binder::PlanLateralJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
                                                    vector<CorrelatedColumnInfo> &correlated, JoinType join_type,
                                                    unique_ptr<Expression> condition) {
	// scan the right operator for correlated columns
	// correlated LATERAL JOIN
	vector<JoinCondition> conditions;
	vector<unique_ptr<Expression>> arbitrary_expressions;
	if (condition) {
		// extract join conditions, if there are any
		LogicalComparisonJoin::ExtractJoinConditions(context, join_type, JoinRefType::REGULAR, left, right,
		                                             std::move(condition), conditions, arbitrary_expressions);
	}

	auto perform_delim = PerformDuplicateElimination(*this, correlated);
	auto delim_join = CreateDuplicateEliminatedJoin(correlated, join_type, std::move(left), perform_delim);

	FlattenDependentJoins flatten(*this, correlated, perform_delim);

	// first we check which logical operators have correlated expressions in the first place
	flatten.DetectCorrelatedExpressions(*right, true);
	// now we push the dependent join down
	auto dependent_join = flatten.PushDownDependentJoin(std::move(right), join_type != JoinType::INNER);

	// now the dependent join is fully eliminated
	// we only need to create the join conditions between the LHS and the RHS
	// fetch the set of columns
	auto plan_columns = dependent_join->GetColumnBindings();

	// in case of a materialized CTE, the output is defined by the second children operator
	if (dependent_join->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		plan_columns = dependent_join->children[1]->GetColumnBindings();
	}

	// now create the join conditions
	// start off with the conditions that were passed in (if any)
	D_ASSERT(delim_join->conditions.empty());
	delim_join->conditions = std::move(conditions);
	// then add the delim join conditions
	CreateDelimJoinConditions(*delim_join, correlated, plan_columns, flatten.delim_offset, perform_delim);
	delim_join->AddChild(std::move(dependent_join));

	// check if there are any arbitrary expressions left
	if (!arbitrary_expressions.empty()) {
		// we can only evaluate scalar arbitrary expressions for inner joins
		if (join_type != JoinType::INNER) {
			throw BinderException(
			    "Join condition for non-inner LATERAL JOIN must be a comparison between the left and right side");
		}
		auto filter = make_uniq<LogicalFilter>();
		filter->expressions = std::move(arbitrary_expressions);
		filter->AddChild(std::move(delim_join));
		return std::move(filter);
	}
	return std::move(delim_join);
}

} // namespace duckdb
