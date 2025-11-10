#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
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
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/main/settings.hpp"

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

		FunctionBinder function_binder(binder);
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

		bool error_on_multiple_rows = DBConfig::GetSetting<ScalarSubqueryErrorOnMultipleRowsSetting>(binder.context);

		// we push an aggregate that returns the FIRST element
		vector<unique_ptr<Expression>> expressions;
		auto bound = make_uniq<BoundColumnRefExpression>(expr.return_type, ColumnBinding(table_idx, 0));
		vector<unique_ptr<Expression>> first_children;
		first_children.push_back(std::move(bound));

		FunctionBinder function_binder(binder);
		auto first_agg =
		    function_binder.BindAggregateFunction(FirstFunctionGetter::GetFunction(expr.return_type),
		                                          std::move(first_children), nullptr, AggregateType::NON_DISTINCT);

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
		for (idx_t child_idx = 0; child_idx < expr.children.size(); child_idx++) {
			JoinCondition cond;
			cond.left = std::move(expr.children[child_idx]);
			auto &child_type = expr.child_types[child_idx];
			auto &compare_type = expr.child_targets[child_idx];
			cond.right = BoundCastExpression::AddDefaultCastToType(
			    make_uniq<BoundColumnRefExpression>(child_type, plan_columns[child_idx]), compare_type);
			cond.comparison = expr.comparison_type;

			// push collations
			ExpressionBinder::PushCollation(binder.context, cond.left, compare_type);
			ExpressionBinder::PushCollation(binder.context, cond.right, compare_type);

			join->conditions.push_back(std::move(cond));
		}
		root = std::move(join);

		// we replace the original subquery with a BoundColumnRefExpression referring to the mark column
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, ColumnBinding(mark_index, 0));
	}
	}
}

static unique_ptr<LogicalDependentJoin> CreateDuplicateEliminatedJoin(const CorrelatedColumns &correlated_columns,
                                                                      JoinType join_type,
                                                                      unique_ptr<LogicalOperator> original_plan,
                                                                      bool perform_delim) {
	auto delim_join = make_uniq<LogicalDependentJoin>(join_type);
	delim_join->correlated_columns = correlated_columns;
	delim_join->perform_delim = perform_delim;
	delim_join->join_type = join_type;
	delim_join->AddChild(std::move(original_plan));
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		delim_join->duplicate_eliminated_columns.push_back(make_uniq<BoundColumnRefExpression>(col.type, col.binding));
		delim_join->mark_types.push_back(col.type);
	}
	return delim_join;
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

static bool PerformDuplicateElimination(Binder &binder, CorrelatedColumns &correlated_columns) {
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
	correlated_columns.AddColumn(std::move(info));
	correlated_columns.SetDelimIndexToZero();
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
	// also read "Improving Unnesting of Complex Queries"
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

		// We have to store all information required to perform UNNESTING later.
		delim_join->subquery_type = SubqueryType::SCALAR;
		delim_join->any_join = false;

		auto plan_column = plan->GetColumnBindings().back();
		delim_join->AddChild(std::move(plan));
		root = std::move(delim_join);
		// finally push the BoundColumnRefExpression referring to the data element returned by the join
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, plan_column);
	}
	case SubqueryType::EXISTS: {
		// correlated EXISTS query
		// this query is similar to the correlated SCALAR query, except we use a MARK join here
		idx_t mark_index = binder.GenerateTableIndex();
		auto delim_join =
		    CreateDuplicateEliminatedJoin(correlated_columns, JoinType::MARK, std::move(root), perform_delim);

		delim_join->subquery_type = SubqueryType::EXISTS;
		delim_join->mark_index = mark_index;
		delim_join->any_join = true;
		delim_join->AddChild(std::move(plan));
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

		delim_join->subquery_type = SubqueryType::ANY;
		delim_join->mark_index = mark_index;
		delim_join->any_join = true;
		auto &dependent_join = plan;

		if (expr.children.size() > 1) {
			// FIXME: the code to generate the plan here is actually correct
			// the problem is in the hash join - specifically PhysicalHashJoin::InitializeHashTable
			// this contains code that is hard-coded for a single comparison
			// -> (delim_types.size() + 1 == conditions.size())
			// this needs to be generalized to get this to work
			throw NotImplementedException("Correlated IN/ANY/ALL with multiple columns not yet supported");
		}

		delim_join->expression_children = std::move(expr.children);
		delim_join->child_types = expr.child_types;
		delim_join->child_targets = expr.child_targets;
		delim_join->comparison_type = expr.comparison_type;

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
		if (op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE ||
		    op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			auto &rec_cte = op.Cast<LogicalRecursiveCTE>();
			binder.recursive_ctes[rec_cte.table_index] = &op;
		}
		for (idx_t i = 0; i < op.children.size(); i++) {
			root = std::move(op.children[i]);
			D_ASSERT(root);
			VisitOperatorExpressions(op);
			op.children[i] = std::move(root);
		}

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
	auto sub_binder = Binder::CreateBinder(context, this);
	sub_binder->is_outside_flattened = false;
	auto subquery_root = std::move(expr.subquery.plan);
	D_ASSERT(subquery_root);

	// now we actually flatten the subquery
	auto plan = std::move(subquery_root);

	unique_ptr<Expression> result_expression;
	if (!expr.IsCorrelated()) {
		result_expression = PlanUncorrelatedSubquery(*this, expr, root, std::move(plan));
	} else {
		result_expression = PlanCorrelatedSubquery(*this, expr, root, std::move(plan));
	}
	IncreaseDepth();
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
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		auto &subquery = expr.Cast<BoundSubqueryExpression>();
		// subquery node! plan it
		expr_ptr = PlanSubquery(subquery, root);
	}
}

unique_ptr<LogicalOperator> Binder::PlanLateralJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
                                                    CorrelatedColumns &correlated, JoinType join_type,
                                                    unique_ptr<Expression> condition) {
	// scan the right operator for correlated columns
	// correlated LATERAL JOIN
	vector<JoinCondition> conditions;
	vector<unique_ptr<Expression>> arbitrary_expressions;
	if (condition) {
		if (condition->HasSubquery()) {
			throw BinderException(*condition, "Subqueries are not supported in LATERAL join conditions");
		}
		// extract join conditions, if there are any
		LogicalComparisonJoin::ExtractJoinConditions(context, join_type, JoinRefType::REGULAR, left, right,
		                                             std::move(condition), conditions, arbitrary_expressions);
	}

	auto perform_delim = PerformDuplicateElimination(*this, correlated);
	auto delim_join = CreateDuplicateEliminatedJoin(correlated, join_type, std::move(left), perform_delim);

	// Store all information required to perform UNNESTING later.
	delim_join->perform_delim = perform_delim;
	delim_join->any_join = false;
	delim_join->propagate_null_values = join_type != JoinType::INNER;
	delim_join->is_lateral_join = true;
	delim_join->arbitrary_expressions = std::move(arbitrary_expressions);
	delim_join->conditions = std::move(conditions);
	delim_join->AddChild(std::move(right));
	return std::move(delim_join);
}

} // namespace duckdb
