#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_streaming_window.hpp"
#include "duckdb/execution/operator/aggregate/physical_window.hpp"
#include "duckdb/execution/operator/join/physical_asof_join.hpp"
#include "duckdb/execution/operator/join/physical_iejoin.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

optional_ptr<PhysicalOperator>
PhysicalPlanGenerator::PlanAsOfLoopJoin(LogicalComparisonJoin &op, PhysicalOperator &probe, PhysicalOperator &build) {

	// Plan a inverse nested loop join, then aggregate the values to choose the optimal match for each probe row.
	// Use a row number primary key to handle duplicate probe values.
	// aggregate the fields to produce at most one match per probe row,
	// then project the columns back into the correct order and drop the primary key.
	//
	//		 ∏ * \ pk
	//		 |
	//		 Γ pk;first(P),arg_xxx(B,inequality)
	//		 |
	//		 ∏ *,inequality
	//		 |
	//       ⨝ swapped
	//     /   \ 
	//    B     W pk:row_number
	//          |
	//          P

	LogicalComparisonJoin join_op(InverseJoinType(op.join_type));

	join_op.types = op.children[1]->types;
	const auto &probe_types = op.children[0]->types;
	join_op.types.insert(join_op.types.end(), probe_types.begin(), probe_types.end());

	//	Fill in the projection maps to simplify the code below
	//	Since NLJ doesn't support projection, but ASOF does,
	//	we have to track this carefully...
	join_op.left_projection_map = op.right_projection_map;
	if (join_op.left_projection_map.empty()) {
		for (idx_t i = 0; i < op.children[1]->types.size(); ++i) {
			join_op.left_projection_map.emplace_back(i);
		}
	}

	join_op.right_projection_map = op.left_projection_map;
	if (join_op.right_projection_map.empty()) {
		for (idx_t i = 0; i < op.children[0]->types.size(); ++i) {
			join_op.right_projection_map.emplace_back(i);
		}
	}

	// Project pk
	LogicalType pk_type = LogicalType::BIGINT;
	join_op.types.emplace_back(pk_type);

	auto binder = Binder::CreateBinder(context);
	FunctionBinder function_binder(*binder);
	auto asof_idx = op.conditions.size();
	string arg_min_max;
	for (idx_t i = 0; i < op.conditions.size(); ++i) {
		const auto &cond = op.conditions[i];
		JoinCondition nested_cond;
		nested_cond.left = cond.right->Copy();
		nested_cond.right = cond.left->Copy();
		if (!nested_cond.left || !nested_cond.right) {
			return nullptr;
		}
		nested_cond.comparison = FlipComparisonExpression(cond.comparison);
		join_op.conditions.emplace_back(std::move(nested_cond));
		switch (cond.comparison) {
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHAN:
			D_ASSERT(asof_idx == op.conditions.size());
			asof_idx = i;
			arg_min_max = "arg_max";
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_LESSTHAN:
			D_ASSERT(asof_idx == op.conditions.size());
			asof_idx = i;
			arg_min_max = "arg_min";
			break;
		default:
			break;
		}
	}

	//	NLJ does not support some join types
	switch (join_op.join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
	case JoinType::INNER:
	case JoinType::RIGHT:
		// Unfortunately, this does not check all the join types...
		if (!PhysicalNestedLoopJoin::IsSupported(op.conditions, op.join_type)) {
			return nullptr;
		}
		break;
	case JoinType::OUTER:
	case JoinType::LEFT:
		//	RIGHT ASOF JOINs produce the entire build table and would require grouping on all build rows,
		//	which defeats the purpose of this optimisation.
	default:
		return nullptr;
	}

	QueryErrorContext error_context;
	auto arg_min_max_func = binder->GetCatalogEntry(CatalogType::SCALAR_FUNCTION_ENTRY, SYSTEM_CATALOG, DEFAULT_SCHEMA,
	                                                arg_min_max, OnEntryNotFound::RETURN_NULL, error_context);
	//	Can't find the arg_min/max aggregate we need, so give up before we break anything.
	if (!arg_min_max_func || arg_min_max_func->type != CatalogType::AGGREGATE_FUNCTION_ENTRY) {
		return nullptr;
	}
	auto &arg_min_max_entry = arg_min_max_func->Cast<AggregateFunctionCatalogEntry>();

	// PhysicalHashAggregate requires that the arguments to aggregate functions be bound references,
	// so we Project the (shared) ordering argument on the end of the join results.
	vector<unique_ptr<Expression>> comp_list;
	for (const auto &col_type : join_op.types) {
		const auto col_idx = comp_list.size();
		comp_list.emplace_back(make_uniq<BoundReferenceExpression>(col_type, col_idx));
	}
	vector<LogicalType> comp_types = join_op.types;
	auto comp_expr = op.conditions[asof_idx].right->Copy();
	comp_types.emplace_back(comp_expr->return_type);
	comp_list.emplace_back(std::move(comp_expr));

	//	Bind the aggregates first so we can abort safely if we can't find one.
	vector<LogicalType> aggr_types(1, pk_type);

	// Wrap all the projected non-pk probe fields in `first` aggregates;
	vector<unique_ptr<Expression>> aggregates;
	for (const auto &i : join_op.right_projection_map) {
		const auto col_idx = op.children[1]->types.size() + i;
		const auto col_type = join_op.types[col_idx];
		aggr_types.emplace_back(col_type);

		vector<unique_ptr<Expression>> aggr_children;
		auto col_ref = make_uniq<BoundReferenceExpression>(col_type, col_idx);
		aggr_children.push_back(std::move(col_ref));

		auto first_aggregate = FirstFunctionGetter::GetFunction(col_type);
		auto aggr_expr = make_uniq<BoundAggregateExpression>(std::move(first_aggregate), std::move(aggr_children),
		                                                     nullptr, nullptr, AggregateType::NON_DISTINCT);
		D_ASSERT(col_type == aggr_expr->return_type);
		aggregates.emplace_back(std::move(aggr_expr));
	}

	// Wrap all the projected build fields in `arg_max/min` aggregates using the inequality ordering;
	// We are doing all this first in case we can't find a matching function.
	for (const auto &col_idx : join_op.left_projection_map) {
		const auto col_type = join_op.types[col_idx];
		aggr_types.emplace_back(col_type);

		vector<unique_ptr<Expression>> aggr_children;
		auto col_ref = make_uniq<BoundReferenceExpression>(col_type, col_idx);
		aggr_children.push_back(std::move(col_ref));
		auto comp_expr = make_uniq<BoundReferenceExpression>(comp_types.back(), comp_types.size() - 1);
		aggr_children.push_back(std::move(comp_expr));
		vector<LogicalType> child_types;
		for (const auto &child : aggr_children) {
			child_types.emplace_back(child->return_type);
		}

		auto &func = arg_min_max_entry;
		ErrorData error;
		auto best_function = function_binder.BindFunction(func.name, func.functions, child_types, error);
		if (!best_function.IsValid()) {
			return nullptr;
		}
		auto bound_function = func.functions.GetFunctionByOffset(best_function.GetIndex());
		auto aggr_expr = function_binder.BindAggregateFunction(bound_function, std::move(aggr_children), nullptr,
		                                                       AggregateType::NON_DISTINCT);
		D_ASSERT(col_type == aggr_expr->return_type);
		aggregates.emplace_back(std::move(aggr_expr));
	}

	// Add a synthetic primary integer key to the probe relation using streaming windowing.
	vector<unique_ptr<Expression>> window_select;
	auto pk = make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_ROW_NUMBER, pk_type, nullptr, nullptr);
	pk->start = WindowBoundary::UNBOUNDED_PRECEDING;
	pk->end = WindowBoundary::CURRENT_ROW_ROWS;
	pk->alias = "row_number";
	window_select.emplace_back(std::move(pk));

	auto window_types = probe.GetTypes();
	window_types.emplace_back(pk_type);

	idx_t probe_cardinality = op.children[0]->EstimateCardinality(context);
	auto &window = Make<PhysicalStreamingWindow>(window_types, std::move(window_select), probe_cardinality);
	window.children.emplace_back(probe);

	auto &join = Make<PhysicalNestedLoopJoin>(join_op, build, window, std::move(join_op.conditions), join_op.join_type,
	                                          probe_cardinality);

	// Plan a projection of the compare column
	auto &comp_proj = Make<PhysicalProjection>(std::move(comp_types), std::move(comp_list), probe_cardinality);
	comp_proj.children.emplace_back(join);

	// Plan an aggregation on the output of the join, grouping by key;
	// TODO: Can we make it perfect?
	// Note that the NLJ produced all fields, but only the projected ones were aggregated
	vector<unique_ptr<Expression>> groups;
	auto pk_ref = make_uniq<BoundReferenceExpression>(pk_type, join_op.types.size() - 1);
	groups.emplace_back(std::move(pk_ref));

	auto &aggr =
	    Make<PhysicalHashAggregate>(context, aggr_types, std::move(aggregates), std::move(groups), probe_cardinality);
	aggr.children.emplace_back(comp_proj);

	// Project away primary/grouping key
	// The aggregates were generated in the output order of the original ASOF,
	// so we just have to shift away the pk
	vector<unique_ptr<Expression>> project_list;
	for (column_t i = 1; i < aggr.GetTypes().size(); ++i) {
		auto col_ref = make_uniq<BoundReferenceExpression>(aggr.GetTypes()[i], i);
		project_list.emplace_back(std::move(col_ref));
	}

	auto &proj = Make<PhysicalProjection>(op.types, std::move(project_list), probe_cardinality);
	proj.children.emplace_back(aggr);
	return proj;
}

PhysicalOperator &PhysicalPlanGenerator::PlanAsOfJoin(LogicalComparisonJoin &op) {
	// now visit the children
	D_ASSERT(op.children.size() == 2);
	idx_t lhs_cardinality = op.children[0]->EstimateCardinality(context);
	idx_t rhs_cardinality = op.children[1]->EstimateCardinality(context);
	auto &left = CreatePlan(*op.children[0]);
	auto &right = CreatePlan(*op.children[1]);

	//	Validate
	vector<idx_t> equi_indexes;
	auto asof_idx = op.conditions.size();
	for (size_t c = 0; c < op.conditions.size(); ++c) {
		auto &cond = op.conditions[c];
		switch (cond.comparison) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			equi_indexes.emplace_back(c);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_LESSTHAN:
			D_ASSERT(asof_idx == op.conditions.size());
			asof_idx = c;
			break;
		default:
			throw InternalException("Invalid ASOF JOIN comparison");
		}
	}
	D_ASSERT(asof_idx < op.conditions.size());

	auto &config = ClientConfig::GetConfig(context);
	if (!config.force_asof_iejoin) {
		if (op.children[0]->has_estimated_cardinality && lhs_cardinality <= config.asof_loop_join_threshold) {
			auto result = PlanAsOfLoopJoin(op, left, right);
			if (result) {
				return *result;
			}
		}
		return Make<PhysicalAsOfJoin>(op, left, right);
	}

	//	Strip extra column from rhs projections
	auto &right_projection_map = op.right_projection_map;
	if (right_projection_map.empty()) {
		const auto right_count = right.GetTypes().size();
		right_projection_map.reserve(right_count);
		for (column_t i = 0; i < right_count; ++i) {
			right_projection_map.emplace_back(i);
		}
	}

	//	Debug implementation: IEJoin of Window
	//	LEAD(asof_column, 1, infinity) OVER (PARTITION BY equi_column... ORDER BY asof_column) AS asof_end
	auto &asof_comp = op.conditions[asof_idx];
	auto &asof_column = asof_comp.right;
	auto asof_type = asof_column->return_type;
	auto asof_end = make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_LEAD, asof_type, nullptr, nullptr);
	asof_end->children.emplace_back(asof_column->Copy());
	// TODO: If infinities are not supported for a type, fake them by looking at LHS statistics?
	asof_end->offset_expr = make_uniq<BoundConstantExpression>(Value::BIGINT(1));
	for (auto equi_idx : equi_indexes) {
		asof_end->partitions.emplace_back(op.conditions[equi_idx].right->Copy());
	}
	switch (asof_comp.comparison) {
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
		asof_end->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, asof_column->Copy());
		asof_end->default_expr = make_uniq<BoundConstantExpression>(Value::Infinity(asof_type));
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHAN:
		asof_end->orders.emplace_back(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, asof_column->Copy());
		asof_end->default_expr = make_uniq<BoundConstantExpression>(Value::NegativeInfinity(asof_type));
		break;
	default:
		throw InternalException("Invalid ASOF JOIN ordering for WINDOW");
	}

	asof_end->start = WindowBoundary::UNBOUNDED_PRECEDING;
	asof_end->end = WindowBoundary::CURRENT_ROW_ROWS;

	vector<unique_ptr<Expression>> window_select;
	window_select.emplace_back(std::move(asof_end));

	auto &window_types = op.children[1]->types;
	window_types.emplace_back(asof_type);

	auto &window = Make<PhysicalWindow>(window_types, std::move(window_select), rhs_cardinality);
	window.children.emplace_back(right);

	// IEJoin(left, window, conditions || asof_comp ~op asof_end)
	JoinCondition asof_upper;
	asof_upper.left = asof_comp.left->Copy();
	asof_upper.right = make_uniq<BoundReferenceExpression>(asof_type, window_types.size() - 1);
	switch (asof_comp.comparison) {
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		asof_upper.comparison = ExpressionType::COMPARE_LESSTHAN;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		asof_upper.comparison = ExpressionType::COMPARE_LESSTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		asof_upper.comparison = ExpressionType::COMPARE_GREATERTHAN;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		asof_upper.comparison = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		break;
	default:
		throw InternalException("Invalid ASOF JOIN comparison for IEJoin");
	}

	op.conditions.emplace_back(std::move(asof_upper));
	return Make<PhysicalIEJoin>(op, left, window, std::move(op.conditions), op.join_type, lhs_cardinality);
}

} // namespace duckdb
