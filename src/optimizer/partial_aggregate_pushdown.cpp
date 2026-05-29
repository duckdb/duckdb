#include "duckdb/optimizer/partial_aggregate_pushdown.hpp"

#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

PartialAggregatePushdown::PartialAggregatePushdown(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

struct PartialAggregatePushdownHeuristics {
	static constexpr idx_t MIN_DIMENSION_GROUPS = 4;
	static constexpr idx_t MIN_AGGREGATE_TO_DIMENSION_RATIO = 4;
	static constexpr idx_t MAX_JOIN_SELECTIVITY_INV = 8;
	static constexpr idx_t MAX_EXTRA_LOWER_GROUPS = 1;
};

struct PartialAggregatePushdownInfo {
	idx_t aggregate_side;
	idx_t dimension_side;
	TableIndex lower_group_index;
	TableIndex lower_aggregate_index;
	idx_t join_key_count;
	unordered_set<TableIndex> side_bindings[2];
	vector<ColumnBinding> lower_group_bindings;
	column_binding_map_t<ColumnBinding> lower_group_map;
	column_binding_map_t<LogicalType> lower_group_types;
};

static bool IsSubset(const unordered_set<TableIndex> &bindings, const unordered_set<TableIndex> &side_bindings) {
	for (auto &binding : bindings) {
		if (side_bindings.find(binding) == side_bindings.end()) {
			return false;
		}
	}
	return true;
}

static unordered_set<TableIndex> GetExpressionBindings(const Expression &expr) {
	unordered_set<TableIndex> bindings;
	LogicalJoin::GetExpressionBindings(expr, bindings);
	return bindings;
}

static bool GetColumnBinding(const Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	binding = expr.Cast<BoundColumnRefExpression>().binding;
	return true;
}

static bool IsSupportedAggregate(const BoundAggregateExpression &expr) {
	if (expr.IsDistinct() || expr.filter || expr.order_bys) {
		return false;
	}
	if (expr.children.size() != 1) {
		return false;
	}
	if (!expr.function.HasGetStateTypeCallback()) {
		return false;
	}
	return true;
}

static bool ContainsAggregateInput(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsAggregateInput(*child)) {
			return true;
		}
	}
	return false;
}

static bool GetPushdownOperators(LogicalOperator &op, LogicalAggregate *&aggr, LogicalComparisonJoin *&join) {
	if (op.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || op.children.size() != 1) {
		return false;
	}
	aggr = &op.Cast<LogicalAggregate>();
	if (!aggr->grouping_functions.empty() || aggr->groups.empty() || aggr->expressions.empty()) {
		return false;
	}
	auto &child = *op.children[0];
	if (child.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	join = &child.Cast<LogicalComparisonJoin>();
	return join->join_type == JoinType::INNER && !join->HasProjectionMap() && join->children.size() == 2 &&
	       !join->conditions.empty();
}

static bool GetExpressionSide(const Expression &expr, const PartialAggregatePushdownInfo &info, idx_t &side) {
	auto bindings = GetExpressionBindings(expr);
	if (bindings.empty()) {
		return false;
	}
	if (IsSubset(bindings, info.side_bindings[0])) {
		side = 0;
		return true;
	}
	if (IsSubset(bindings, info.side_bindings[1])) {
		side = 1;
		return true;
	}
	return false;
}

static bool FindAggregateSide(const LogicalAggregate &aggr, PartialAggregatePushdownInfo &info) {
	optional_idx aggregate_side;
	for (auto &expr : aggr.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		auto &aggregate = expr->Cast<BoundAggregateExpression>();
		if (!IsSupportedAggregate(aggregate)) {
			return false;
		}
		idx_t side;
		if (!GetExpressionSide(*aggregate.children[0], info, side)) {
			return false;
		}
		if (!aggregate_side.IsValid()) {
			aggregate_side = side;
		} else if (aggregate_side.GetIndex() != side) {
			return false;
		}
	}
	if (!aggregate_side.IsValid()) {
		return false;
	}
	info.aggregate_side = aggregate_side.GetIndex();
	info.dimension_side = 1 - info.aggregate_side;
	return true;
}

static bool PassesCardinalityHeuristic(const LogicalComparisonJoin &join, const PartialAggregatePushdownInfo &info) {
	auto &aggregate_child = *join.children[info.aggregate_side];
	auto &dimension_child = *join.children[info.dimension_side];
	if (!aggregate_child.has_estimated_cardinality || !dimension_child.has_estimated_cardinality) {
		return true;
	}
	if (aggregate_child.estimated_cardinality <
	    PartialAggregatePushdownHeuristics::MIN_AGGREGATE_TO_DIMENSION_RATIO * dimension_child.estimated_cardinality) {
		return false;
	}
	if (join.has_estimated_cardinality &&
	    join.estimated_cardinality * PartialAggregatePushdownHeuristics::MAX_JOIN_SELECTIVITY_INV <
	        aggregate_child.estimated_cardinality) {
		return false;
	}
	return true;
}

static bool GetJoinSideExpressions(JoinCondition &condition, const PartialAggregatePushdownInfo &info,
                                   unique_ptr<Expression> *&aggregate_expr, unique_ptr<Expression> *&dimension_expr) {
	if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	auto left_bindings = GetExpressionBindings(condition.GetLHS());
	auto right_bindings = GetExpressionBindings(condition.GetRHS());
	if (left_bindings.empty() || right_bindings.empty()) {
		return false;
	}
	if (IsSubset(left_bindings, info.side_bindings[info.aggregate_side]) &&
	    IsSubset(right_bindings, info.side_bindings[info.dimension_side])) {
		aggregate_expr = &condition.LeftReference();
		dimension_expr = &condition.RightReference();
		return true;
	}
	if (IsSubset(right_bindings, info.side_bindings[info.aggregate_side]) &&
	    IsSubset(left_bindings, info.side_bindings[info.dimension_side])) {
		aggregate_expr = &condition.RightReference();
		dimension_expr = &condition.LeftReference();
		return true;
	}
	return false;
}

static bool ValidateJoinConditions(LogicalComparisonJoin &join, const PartialAggregatePushdownInfo &info) {
	for (auto &condition : join.conditions) {
		unique_ptr<Expression> *aggregate_expr;
		unique_ptr<Expression> *dimension_expr;
		if (!GetJoinSideExpressions(condition, info, aggregate_expr, dimension_expr)) {
			return false;
		}
		ColumnBinding join_key;
		if (!GetColumnBinding(**aggregate_expr, join_key)) {
			return false;
		}
	}
	return true;
}

static bool HasWideDimensionGroups(const LogicalAggregate &aggr, const PartialAggregatePushdownInfo &info) {
	idx_t dimension_group_count = 0;
	for (auto &group : aggr.groups) {
		ColumnBinding group_binding;
		if (!GetColumnBinding(*group, group_binding)) {
			return false;
		}
		idx_t side;
		if (!GetExpressionSide(*group, info, side)) {
			return false;
		}
		if (side == info.dimension_side) {
			dimension_group_count++;
		}
	}
	return dimension_group_count >= PartialAggregatePushdownHeuristics::MIN_DIMENSION_GROUPS;
}

static bool AnalyzePushdown(LogicalAggregate &aggr, LogicalComparisonJoin &join, PartialAggregatePushdownInfo &info) {
	LogicalJoin::GetTableReferences(*join.children[0], info.side_bindings[0]);
	LogicalJoin::GetTableReferences(*join.children[1], info.side_bindings[1]);
	if (!FindAggregateSide(aggr, info)) {
		return false;
	}
	if (ContainsAggregateInput(*join.children[info.aggregate_side])) {
		return false;
	}
	if (info.side_bindings[info.dimension_side].size() != 1) {
		return false;
	}
	if (!PassesCardinalityHeuristic(join, info)) {
		return false;
	}
	if (!ValidateJoinConditions(join, info)) {
		return false;
	}
	return HasWideDimensionGroups(aggr, info);
}

static void AddLowerGroup(PartialAggregatePushdownInfo &info, ColumnBinding binding, const LogicalType &type) {
	if (info.lower_group_map.find(binding) != info.lower_group_map.end()) {
		return;
	}
	auto lower_binding = ColumnBinding(info.lower_group_index, ProjectionIndex(info.lower_group_bindings.size()));
	info.lower_group_bindings.push_back(binding);
	info.lower_group_map[binding] = lower_binding;
	info.lower_group_types[binding] = type;
}

static void BuildLowerGroupMap(LogicalAggregate &aggr, LogicalComparisonJoin &join,
                               PartialAggregatePushdownInfo &info) {
	for (auto &condition : join.conditions) {
		unique_ptr<Expression> *aggregate_expr;
		unique_ptr<Expression> *dimension_expr;
		GetJoinSideExpressions(condition, info, aggregate_expr, dimension_expr);
		ColumnBinding binding;
		GetColumnBinding(**aggregate_expr, binding);
		AddLowerGroup(info, binding, (*aggregate_expr)->GetReturnType());
	}
	info.join_key_count = info.lower_group_bindings.size();
	for (auto &group : aggr.groups) {
		auto &group_ref = group->Cast<BoundColumnRefExpression>();
		if (info.lower_group_map.find(group_ref.binding) != info.lower_group_map.end()) {
			continue;
		}
		idx_t side;
		GetExpressionSide(*group, info, side);
		if (side == info.aggregate_side) {
			AddLowerGroup(info, group_ref.binding, group->GetReturnType());
		}
	}
}

static bool PassesLowerGroupHeuristic(const PartialAggregatePushdownInfo &info) {
	return info.lower_group_bindings.size() <=
	       info.join_key_count + PartialAggregatePushdownHeuristics::MAX_EXTRA_LOWER_GROUPS;
}

static bool BindPushdownAggregates(ClientContext &context, LogicalAggregate &aggr, TableIndex lower_aggregate_index,
                                   vector<unique_ptr<Expression>> &lower_aggregates,
                                   vector<unique_ptr<Expression>> &upper_aggregates) {
	auto combine_function = CombineAggrFun::GetFunction();
	FunctionBinder function_binder(context);

	for (idx_t i = 0; i < aggr.expressions.size(); i++) {
		auto aggregate_copy = unique_ptr_cast<Expression, BoundAggregateExpression>(aggr.expressions[i]->Copy());
		auto lower_aggregate = ExportAggregateFunction::Bind(std::move(aggregate_copy));
		auto lower_type = lower_aggregate->GetReturnType();
		if (lower_type.id() != LogicalTypeId::AGGREGATE_STATE) {
			return false;
		}

		vector<unique_ptr<Expression>> arguments;
		auto lower_binding = ColumnBinding(lower_aggregate_index, ProjectionIndex(i));
		arguments.push_back(make_uniq<BoundColumnRefExpression>(lower_type, lower_binding));
		auto upper_aggregate = function_binder.BindAggregateFunction(combine_function, std::move(arguments));
		if (upper_aggregate->GetReturnType().id() != LogicalTypeId::AGGREGATE_STATE) {
			return false;
		}
		lower_aggregates.push_back(std::move(lower_aggregate));
		upper_aggregates.push_back(std::move(upper_aggregate));
	}
	return true;
}

static vector<unique_ptr<Expression>> CreateLowerGroups(const PartialAggregatePushdownInfo &info) {
	vector<unique_ptr<Expression>> lower_groups;
	for (auto &binding : info.lower_group_bindings) {
		auto type = info.lower_group_types.at(binding);
		lower_groups.push_back(make_uniq<BoundColumnRefExpression>(type, binding));
	}
	return lower_groups;
}

static unique_ptr<LogicalAggregate> CreateLowerAggregate(LogicalAggregate &aggr, LogicalComparisonJoin &join,
                                                         PartialAggregatePushdownInfo &info,
                                                         vector<unique_ptr<Expression>> lower_aggregates) {
	auto lower_aggr =
	    make_uniq<LogicalAggregate>(info.lower_group_index, info.lower_aggregate_index, std::move(lower_aggregates));
	lower_aggr->groups = CreateLowerGroups(info);
	lower_aggr->children.push_back(std::move(join.children[info.aggregate_side]));
	lower_aggr->ResolveOperatorTypes();
	lower_aggr->estimated_cardinality = aggr.estimated_cardinality;
	lower_aggr->has_estimated_cardinality = aggr.has_estimated_cardinality;
	return lower_aggr;
}

static unique_ptr<LogicalComparisonJoin> CreateJoin(LogicalComparisonJoin &join, PartialAggregatePushdownInfo &info,
                                                    unique_ptr<LogicalAggregate> lower_aggr) {
	auto new_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	if (info.aggregate_side == 0) {
		new_join->children.push_back(std::move(lower_aggr));
		new_join->children.push_back(std::move(join.children[info.dimension_side]));
	} else {
		new_join->children.push_back(std::move(join.children[info.dimension_side]));
		new_join->children.push_back(std::move(lower_aggr));
	}

	for (auto &condition : join.conditions) {
		unique_ptr<Expression> *aggregate_expr;
		unique_ptr<Expression> *dimension_expr;
		GetJoinSideExpressions(condition, info, aggregate_expr, dimension_expr);
		ColumnBinding join_key;
		GetColumnBinding(**aggregate_expr, join_key);
		auto lower_binding = info.lower_group_map[join_key];
		auto lower_type = new_join->children[info.aggregate_side]->types[lower_binding.column_index.GetIndex()];
		auto lower_expr = make_uniq<BoundColumnRefExpression>(lower_type, lower_binding);
		if (info.aggregate_side == 0) {
			new_join->conditions.emplace_back(std::move(lower_expr), (*dimension_expr)->Copy(),
			                                  ExpressionType::COMPARE_EQUAL);
		} else {
			new_join->conditions.emplace_back((*dimension_expr)->Copy(), std::move(lower_expr),
			                                  ExpressionType::COMPARE_EQUAL);
		}
	}
	new_join->ResolveOperatorTypes();
	new_join->estimated_cardinality = join.estimated_cardinality;
	new_join->has_estimated_cardinality = join.has_estimated_cardinality;
	return new_join;
}

static vector<unique_ptr<Expression>> CreateUpperGroups(LogicalAggregate &aggr, LogicalComparisonJoin &new_join,
                                                        const PartialAggregatePushdownInfo &info) {
	vector<unique_ptr<Expression>> upper_groups;
	for (auto &group : aggr.groups) {
		auto &group_ref = group->Cast<BoundColumnRefExpression>();
		auto entry = info.lower_group_map.find(group_ref.binding);
		if (entry == info.lower_group_map.end()) {
			upper_groups.push_back(group->Copy());
			continue;
		}
		auto type = new_join.children[info.aggregate_side]->types[entry->second.column_index.GetIndex()];
		upper_groups.push_back(make_uniq<BoundColumnRefExpression>(type, entry->second));
	}
	return upper_groups;
}

static unique_ptr<LogicalAggregate> CreateUpperAggregate(LogicalAggregate &aggr,
                                                         unique_ptr<LogicalComparisonJoin> new_join,
                                                         const PartialAggregatePushdownInfo &info,
                                                         vector<unique_ptr<Expression>> upper_aggregates) {
	auto upper_aggr = make_uniq<LogicalAggregate>(aggr.group_index, aggr.aggregate_index, std::move(upper_aggregates));
	upper_aggr->groups = CreateUpperGroups(aggr, *new_join, info);
	// Preserve ROLLUP / CUBE / GROUPING SETS. CreateUpperGroups builds the
	// upper groups in the same order as `aggr.groups`, so the indices stored
	// in grouping_sets remain valid. Without this copy the upper aggregate
	// collapses to a single set and silently over-aggregates.
	upper_aggr->grouping_sets = aggr.grouping_sets;
	upper_aggr->grouping_functions = aggr.grouping_functions;
	upper_aggr->children.push_back(std::move(new_join));
	upper_aggr->ResolveOperatorTypes();
	upper_aggr->estimated_cardinality = aggr.estimated_cardinality;
	upper_aggr->has_estimated_cardinality = aggr.has_estimated_cardinality;
	return upper_aggr;
}

static unique_ptr<LogicalProjection> CreateFinalProjection(Optimizer &optimizer, LogicalAggregate &aggr,
                                                           unique_ptr<LogicalAggregate> upper_aggr,
                                                           column_binding_map_t<ColumnBinding> &replacement_map) {
	const auto proj_index = optimizer.binder.GenerateTableIndex();
	const auto group_count = aggr.groups.size();
	vector<unique_ptr<Expression>> projection_expressions;
	projection_expressions.reserve(group_count + aggr.expressions.size());

	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		auto group_binding = ColumnBinding(upper_aggr->group_index, ProjectionIndex(group_idx));
		replacement_map[group_binding] = ColumnBinding(proj_index, ProjectionIndex(group_idx));
		projection_expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(upper_aggr->types[group_idx], group_binding));
	}

	for (idx_t aggr_idx = 0; aggr_idx < aggr.expressions.size(); aggr_idx++) {
		auto aggregate_binding = ColumnBinding(upper_aggr->aggregate_index, ProjectionIndex(aggr_idx));
		replacement_map[aggregate_binding] = ColumnBinding(proj_index, ProjectionIndex(group_count + aggr_idx));
		auto aggregate_type = upper_aggr->types[group_count + aggr_idx];
		auto aggregate_ref = make_uniq<BoundColumnRefExpression>(aggregate_type, aggregate_binding);
		auto final_expression = optimizer.BindScalarFunction("finalize", std::move(aggregate_ref));
		if (final_expression->GetReturnType() != aggr.expressions[aggr_idx]->GetReturnType()) {
			return nullptr;
		}
		projection_expressions.push_back(std::move(final_expression));
	}

	auto projection = make_uniq<LogicalProjection>(proj_index, std::move(projection_expressions));
	if (upper_aggr->has_estimated_cardinality) {
		projection->SetEstimatedCardinality(upper_aggr->estimated_cardinality);
	}
	projection->children.push_back(std::move(upper_aggr));
	projection->ResolveOperatorTypes();
	return projection;
}

void PartialAggregatePushdown::VisitOperator(unique_ptr<LogicalOperator> &op) {
	LogicalOperatorVisitor::VisitOperator(op);
	TryPushdownAggregate(op);
}

unique_ptr<Expression> PartialAggregatePushdown::VisitReplace(BoundColumnRefExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	auto entry = replacement_map.find(expr.binding);
	if (entry != replacement_map.end()) {
		expr.binding = entry->second;
	}
	return nullptr;
}

bool PartialAggregatePushdown::TryPushdownAggregate(unique_ptr<LogicalOperator> &op) {
	LogicalAggregate *aggr;
	LogicalComparisonJoin *join;
	if (!GetPushdownOperators(*op, aggr, join)) {
		return false;
	}

	PartialAggregatePushdownInfo info;
	if (!AnalyzePushdown(*aggr, *join, info)) {
		return false;
	}
	info.lower_group_index = optimizer.binder.GenerateTableIndex();
	info.lower_aggregate_index = optimizer.binder.GenerateTableIndex();
	BuildLowerGroupMap(*aggr, *join, info);
	if (!PassesLowerGroupHeuristic(info)) {
		return false;
	}

	vector<unique_ptr<Expression>> lower_aggregates;
	vector<unique_ptr<Expression>> upper_aggregates;
	if (!BindPushdownAggregates(optimizer.context, *aggr, info.lower_aggregate_index, lower_aggregates,
	                            upper_aggregates)) {
		return false;
	}

	auto lower_aggr = CreateLowerAggregate(*aggr, *join, info, std::move(lower_aggregates));
	auto new_join = CreateJoin(*join, info, std::move(lower_aggr));
	auto upper_aggr = CreateUpperAggregate(*aggr, std::move(new_join), info, std::move(upper_aggregates));
	auto final_projection = CreateFinalProjection(optimizer, *aggr, std::move(upper_aggr), replacement_map);
	if (!final_projection) {
		return false;
	}
	op = std::move(final_projection);
	return true;
}

} // namespace duckdb
