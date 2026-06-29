#include "duckdb/optimizer/partial_aggregate_pushdown.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

PartialAggregatePushdown::PartialAggregatePushdown(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

struct PartialAggregatePushdownHeuristics {
	static constexpr idx_t MIN_AGGREGATE_TO_DIMENSION_RATIO = 4;
	static constexpr idx_t MAX_EXTRA_LOWER_GROUPS = 1;
	// The join must retain at least this fraction of the aggregate side (else the dimension is selective and
	// pre-aggregating wastes work on rows the join discards). 0.7 sits between the regressing TPC queries
	// (join keeps <=0.58 of the agg side) and clean FK joins (the estimator scores those ~0.8-1.0).
	static constexpr double MIN_JOIN_RETENTION = 0.7;
};

struct PartialAggregatePushdownInfo {
	idx_t aggregate_side;
	idx_t dimension_side;
	TableIndex lower_group_index;
	TableIndex lower_aggregate_index;
	TableIndex upper_group_index;
	TableIndex upper_aggregate_index;
	idx_t join_key_count;
	unordered_set<TableIndex> side_bindings[2];
	vector<unique_ptr<Expression>> lower_groups;
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
	binding = expr.Cast<BoundColumnRefExpression>().Binding();
	return true;
}

// Carry an operator's estimated cardinality over to a rewritten operator built from it.
static void CopyCardinality(LogicalOperator &dst, const LogicalOperator &src) {
	dst.estimated_cardinality = src.estimated_cardinality;
	dst.has_estimated_cardinality = src.has_estimated_cardinality;
}

static bool IsSupportedAggregate(const BoundAggregateExpression &expr) {
	if (expr.IsDistinct() || expr.GetFilter() || expr.GetOrderBys()) {
		return false;
	}
	if (expr.StateExportMode() != AggregateStateExportMode::NONE) {
		// the aggregate already exports its state - we cannot push it down again (and finalizing the
		// re-exported state would not round-trip back to the original return type)
		return false;
	}
	if (expr.GetChildren().size() > 1) {
		// only nullary (count(*)) and single-argument aggregates are pushable
		return false;
	}
	if (!expr.Function().HasGetStateTypeCallback()) {
		return false;
	}
	if (expr.Function().GetOrderDependent() == AggregateOrderDependent::ORDER_DEPENDENT &&
	    !expr.Function().GetReassociationPrecisionOnly()) {
		// pushing down a partial aggregate changes the order in which values are combined. That is fine when
		// the order dependence is precision-only (e.g. sum/avg over DOUBLE) - reassociating is no less
		// deterministic than parallel grouped aggregation already is. Genuinely order-dependent aggregates
		// (string_agg, first/last, list, and kahan_sum, which is the deterministic FP alternative) are rejected.
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

static bool JoinContainsDelimGet(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return true;
	}
	for (auto &child : op.children) {
		if (JoinContainsDelimGet(*child)) {
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
	if (JoinContainsDelimGet(*join)) {
		// The join feeds a correlated-subquery decorrelation domain (DELIM_GET). Pushing an aggregate into it
		// fights the Deliminator's plan and is a net loss (e.g. TPC-H Q17's inner aggregate). Leave it alone.
		return false;
	}
	return join->join_type == JoinType::INNER && !join->HasProjectionMap() && join->children.size() == 2 &&
	       !join->conditions.empty();
}

static bool GetExpressionSide(const Expression &expr, const unordered_set<TableIndex> (&side_bindings)[2],
                              idx_t &side) {
	auto bindings = GetExpressionBindings(expr);
	if (bindings.empty()) {
		return false;
	}
	if (IsSubset(bindings, side_bindings[0])) {
		side = 0;
		return true;
	}
	if (IsSubset(bindings, side_bindings[1])) {
		side = 1;
		return true;
	}
	return false;
}

// Collect the grouping columns of a side's lower aggregate: that side's join keys, then the original group-by
// columns that live on that side, deduplicated. Writes the group expressions and the original->lower binding/type
// maps into the caller's buffers and returns how many of the groups are join keys (they come first). Shared by
// the one-sided and double-eager paths.
static idx_t CollectLowerSideGroups(TableIndex group_index, const vector<ColumnBinding> &join_keys,
                                    const vector<LogicalType> &join_key_types,
                                    const vector<unique_ptr<Expression>> &aggr_groups, idx_t side,
                                    const unordered_set<TableIndex> (&side_bindings)[2],
                                    vector<unique_ptr<Expression>> &out_groups,
                                    column_binding_map_t<ColumnBinding> &out_map,
                                    column_binding_map_t<LogicalType> &out_types) {
	auto add = [&](ColumnBinding binding, const LogicalType &type) {
		if (out_map.find(binding) != out_map.end()) {
			return;
		}
		out_map[binding] = ColumnBinding(group_index, ProjectionIndex(out_groups.size()));
		out_types[binding] = type;
		out_groups.push_back(make_uniq<BoundColumnRefExpression>(type, binding));
	};
	for (idx_t k = 0; k < join_keys.size(); k++) {
		add(join_keys[k], join_key_types[k]);
	}
	const idx_t join_key_count = out_groups.size();
	for (auto &group : aggr_groups) {
		auto &ref = group->Cast<BoundColumnRefExpression>();
		idx_t s;
		if (GetExpressionSide(*group, side_bindings, s) && s == side) {
			add(ref.Binding(), ref.GetReturnType());
		}
	}
	return join_key_count;
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
		if (aggregate.GetChildren().empty()) {
			// count(*) is side-agnostic: it rides along to whichever side is chosen, and the
			// join multiplicity on the opposite side reconstructs the count after combine
			continue;
		}
		idx_t side;
		if (!GetExpressionSide(*aggregate.GetChildren()[0], info.side_bindings, side)) {
			return false;
		}
		if (!aggregate_side.IsValid()) {
			aggregate_side = side;
		} else if (aggregate_side.GetIndex() != side) {
			return false;
		}
	}
	if (!aggregate_side.IsValid()) {
		// Only side-agnostic aggregates (e.g. count(*)): no measured column pins a side, so the one-sided path
		// has nothing to anchor the pushed state to. The double-eager path handles count(*)-only instead.
		return false;
	}
	info.aggregate_side = aggregate_side.GetIndex();
	info.dimension_side = 1 - info.aggregate_side;
	return true;
}

static bool PassesCardinalityHeuristic(const LogicalComparisonJoin &join, const PartialAggregatePushdownInfo &info) {
	auto &aggregate_child = *join.children[info.aggregate_side];
	auto &dimension_child = *join.children[info.dimension_side];
	if (!aggregate_child.has_estimated_cardinality || !join.has_estimated_cardinality) {
		return true; // no estimates: fall back to the structural checks
	}
	const auto agg_card = aggregate_child.estimated_cardinality;
	const auto join_card = join.estimated_cardinality;
	// Fan-out shape (e.g. fact x fact through a shared key): the join produces more rows than the
	// aggregate side has. Pre-aggregating shrinks what the fan-out replicates - the most beneficial case.
	if (join_card > agg_card) {
		return true;
	}
	// Selective dimension: the join discards a large fraction of the aggregate side. Eager aggregation would
	// then waste work pre-aggregating rows the join drops, which is a net loss even when the per-key collapse
	// looks high (regresses TPC-H 12/17/18/20 and TPC-DS 51). Require the join to retain ~all of the agg side.
	// done in double: estimates can be near idx_t's range for fan-out joins, where an integer multiply would wrap
	if (static_cast<double>(join_card) <
	    PartialAggregatePushdownHeuristics::MIN_JOIN_RETENTION * static_cast<double>(agg_card)) {
		return false;
	}
	if (!dimension_child.has_estimated_cardinality) {
		return true;
	}
	// Clean (non-fanning) FK join: only worth it if the aggregate side collapses, i.e. the fact is materially
	// larger than the number of distinct join keys (bounded by the dimension side). Computed in double like the
	// retention check above: a fan-out dimension subtree can be estimated near idx_t's range, where RATIO*card in
	// idx_t would wrap and corrupt the decision.
	return static_cast<double>(agg_card) >=
	       static_cast<double>(PartialAggregatePushdownHeuristics::MIN_AGGREGATE_TO_DIMENSION_RATIO) *
	           static_cast<double>(dimension_child.estimated_cardinality);
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

static bool HasDimensionGroup(const LogicalAggregate &aggr, const PartialAggregatePushdownInfo &info) {
	idx_t dimension_group_count = 0;
	for (auto &group : aggr.groups) {
		ColumnBinding group_binding;
		if (!GetColumnBinding(*group, group_binding)) {
			return false;
		}
		idx_t side;
		if (!GetExpressionSide(*group, info.side_bindings, side)) {
			return false;
		}
		if (side == info.dimension_side) {
			dimension_group_count++;
		}
	}
	return dimension_group_count > 0; // at least one group must live on the dimension side
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
	if (info.side_bindings[info.dimension_side].empty()) {
		return false;
	}
	if (!PassesCardinalityHeuristic(join, info)) {
		return false;
	}
	if (!ValidateJoinConditions(join, info)) {
		return false;
	}
	return HasDimensionGroup(aggr, info);
}

static void BuildLowerGroupMap(LogicalAggregate &aggr, LogicalComparisonJoin &join,
                               PartialAggregatePushdownInfo &info) {
	// the aggregate-side join keys (validated to be plain columns by ValidateJoinConditions)
	vector<ColumnBinding> join_keys;
	vector<LogicalType> join_key_types;
	for (auto &condition : join.conditions) {
		unique_ptr<Expression> *aggregate_expr;
		unique_ptr<Expression> *dimension_expr;
		GetJoinSideExpressions(condition, info, aggregate_expr, dimension_expr);
		ColumnBinding binding;
		GetColumnBinding(**aggregate_expr, binding);
		join_keys.push_back(binding);
		join_key_types.push_back((*aggregate_expr)->GetReturnType());
	}
	info.join_key_count =
	    CollectLowerSideGroups(info.lower_group_index, join_keys, join_key_types, aggr.groups, info.aggregate_side,
	                           info.side_bindings, info.lower_groups, info.lower_group_map, info.lower_group_types);
}

static bool PassesLowerGroupHeuristic(const PartialAggregatePushdownInfo &info) {
	return info.lower_groups.size() <= info.join_key_count + PartialAggregatePushdownHeuristics::MAX_EXTRA_LOWER_GROUPS;
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
		if (!lower_type.IsAggregateState()) {
			return false;
		}

		vector<unique_ptr<Expression>> arguments;
		auto lower_binding = ColumnBinding(lower_aggregate_index, ProjectionIndex(i));
		arguments.push_back(make_uniq<BoundColumnRefExpression>(lower_type, lower_binding));
		auto upper_aggregate = function_binder.BindAggregateFunction(combine_function, std::move(arguments));
		if (!upper_aggregate->GetReturnType().IsAggregateState()) {
			return false;
		}
		lower_aggregates.push_back(std::move(lower_aggregate));
		upper_aggregates.push_back(std::move(upper_aggregate));
	}
	return true;
}

static unique_ptr<LogicalAggregate> CreateLowerAggregate(LogicalAggregate &aggr, LogicalComparisonJoin &join,
                                                         PartialAggregatePushdownInfo &info,
                                                         vector<unique_ptr<Expression>> lower_aggregates) {
	auto lower_aggr =
	    make_uniq<LogicalAggregate>(info.lower_group_index, info.lower_aggregate_index, std::move(lower_aggregates));
	lower_aggr->groups = std::move(info.lower_groups);
	lower_aggr->children.push_back(std::move(join.children[info.aggregate_side]));
	lower_aggr->ResolveOperatorTypes();
	CopyCardinality(*lower_aggr, aggr);
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
	CopyCardinality(*new_join, join);
	return new_join;
}

static vector<unique_ptr<Expression>> CreateUpperGroups(LogicalAggregate &aggr, LogicalComparisonJoin &new_join,
                                                        const PartialAggregatePushdownInfo &info) {
	vector<unique_ptr<Expression>> upper_groups;
	for (auto &group : aggr.groups) {
		auto &group_ref = group->Cast<BoundColumnRefExpression>();
		auto entry = info.lower_group_map.find(group_ref.Binding());
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
	// Fresh upper indices (distinct from the original aggregate's): the final projection below maps the original
	// indices to itself, so reusing them here would let a re-traversal rewrite the upper aggregate's own outputs.
	auto upper_aggr =
	    make_uniq<LogicalAggregate>(info.upper_group_index, info.upper_aggregate_index, std::move(upper_aggregates));
	upper_aggr->groups = CreateUpperGroups(aggr, *new_join, info);
	// Preserve ROLLUP / CUBE / GROUPING SETS. CreateUpperGroups builds the
	// upper groups in the same order as `aggr.groups`, so the indices stored
	// in grouping_sets remain valid. Without this copy the upper aggregate
	// collapses to a single set and silently over-aggregates.
	upper_aggr->grouping_sets = aggr.grouping_sets;
	upper_aggr->grouping_functions = aggr.grouping_functions;
	upper_aggr->children.push_back(std::move(new_join));
	upper_aggr->ResolveOperatorTypes();
	CopyCardinality(*upper_aggr, aggr);
	return upper_aggr;
}

// Insert an original->rewritten binding into the replacement map (consumed by VisitReplace on ancestors). The map
// is long-lived across the whole traversal, so we assert its load-bearing invariant: keys are pre-existing aggregate
// bindings and values are freshly-generated projection bindings, so the key set and value set stay disjoint. Today
// that holds for free (GenerateTableIndex values are issued during optimization, strictly after the original keys);
// VisitReplace does a single lookup, so within one pass nothing chains. The assertion guards a future change that
// reused an index: if a value became a key, the post-order re-traversal (VisitOperator re-visits after a push) could
// rewrite a binding a second time (key -> value -> some other value), silently corrupting it
static void AddReplacement(column_binding_map_t<ColumnBinding> &replacement_map, ColumnBinding key,
                           ColumnBinding value) {
#ifdef D_ASSERT_IS_ENABLED
	D_ASSERT(replacement_map.find(key) == replacement_map.end());
	for (auto &entry : replacement_map) {
		D_ASSERT(entry.second != key);  // the new key must not already be a value
		D_ASSERT(entry.first != value); // the new value must not already be a key
	}
#endif
	replacement_map[key] = value;
}

// Build the projection above `upper_aggr` that re-exposes the original aggregate's output columns and records the
// binding remap for ancestors. `build_value(j, ref)` turns a reference to upper_aggr's j-th aggregate column into
// the final expression - finalize(state) for the one-sided path, a cast for double-eager; returning nullptr aborts.
// Both paths share this so the (binding-sensitive) replacement_map keying lives in exactly one place.
template <class BUILD_VALUE>
static unique_ptr<LogicalProjection>
BuildFinalProjection(Optimizer &optimizer, LogicalAggregate &aggr, unique_ptr<LogicalAggregate> upper_aggr,
                     column_binding_map_t<ColumnBinding> &replacement_map, BUILD_VALUE &&build_value) {
	const auto proj_index = optimizer.binder.GenerateTableIndex();
	const auto group_count = aggr.groups.size();
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(group_count + aggr.expressions.size());

	for (idx_t i = 0; i < group_count; i++) {
		AddReplacement(replacement_map, ColumnBinding(aggr.group_index, ProjectionIndex(i)),
		               ColumnBinding(proj_index, ProjectionIndex(i)));
		expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    upper_aggr->types[i], ColumnBinding(upper_aggr->group_index, ProjectionIndex(i))));
	}

	for (idx_t j = 0; j < aggr.expressions.size(); j++) {
		auto ref = make_uniq<BoundColumnRefExpression>(upper_aggr->types[group_count + j],
		                                               ColumnBinding(upper_aggr->aggregate_index, ProjectionIndex(j)));
		auto value = build_value(j, std::move(ref));
		if (!value) {
			return nullptr;
		}
		AddReplacement(replacement_map, ColumnBinding(aggr.aggregate_index, ProjectionIndex(j)),
		               ColumnBinding(proj_index, ProjectionIndex(group_count + j)));
		expressions.push_back(std::move(value));
	}

	auto projection = make_uniq<LogicalProjection>(proj_index, std::move(expressions));
	if (upper_aggr->has_estimated_cardinality) {
		projection->SetEstimatedCardinality(upper_aggr->estimated_cardinality);
	}
	projection->children.push_back(std::move(upper_aggr));
	projection->ResolveOperatorTypes();
	return projection;
}

// Double-eager pushdown:
// Pre-aggregate BOTH join inputs by the join key, then reconstruct each original aggregate above the join by
// scaling one side's partial with the other side's per-key row count. Unlike the one-sided path this collapses
// both inputs to one row per key (ideal for fact x fact through a shared key), at the cost of only supporting
// scalar-decomposable aggregates: count(*), sum, count, min, max (avg arrives pre-split as sum/count).
enum class DoubleEagerKind { COUNT_STAR, SUM, COUNT, MIN, MAX };

struct DoubleEagerAggregate {
	DoubleEagerKind kind;
	idx_t side = 0;              // side that owns the input column (irrelevant for COUNT_STAR)
	ColumnBinding child_binding; // input column binding (unset for COUNT_STAR)
	LogicalType child_type;
	LogicalType return_type; // original aggregate return type (final projection casts back to this)
	idx_t partial_pos = 0;   // position of this aggregate's partial within its side's lower aggregate
};

struct DoubleEagerSide {
	TableIndex group_index;
	TableIndex aggregate_index;
	vector<unique_ptr<Expression>> groups;
	vector<unique_ptr<Expression>> aggregates;     // always starts with count_star() at position 0
	column_binding_map_t<ColumnBinding> group_map; // original group/key binding -> lower group binding
	column_binding_map_t<LogicalType> group_types;
};

static unique_ptr<BoundAggregateExpression> DEBindAggregate(ClientContext &context, const string &name,
                                                            vector<unique_ptr<Expression>> children) {
	auto &entry = Catalog::GetSystemCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(
	    context, Identifier::DefaultSchema(), Identifier(name));
	vector<LogicalType> types;
	for (auto &child : children) {
		types.push_back(child->GetReturnType());
	}
	ErrorData error;
	FunctionBinder function_binder(context);
	auto best = function_binder.BindFunction(entry.name, entry.functions, types, error);
	if (!best.IsValid()) {
		return nullptr;
	}
	auto &func = entry.functions.GetFunctionByOffset(best.GetIndex());
	return function_binder.BindAggregateFunction(func, std::move(children));
}

static bool DEClassify(const BoundAggregateExpression &aggr, DoubleEagerKind &kind) {
	if (aggr.IsDistinct() || aggr.GetFilter() || aggr.GetOrderBys()) {
		return false;
	}
	// Only a genuine distributive aggregate can be reconstructed from per-key partials. The `distributive` property
	// is set by the builtin sum/count/min/max registrations, so a same-named user/extension overload (which does not
	// set it) is rejected here; the name below then only selects the reconstruction formula.
	if (!aggr.Function().GetDistributive()) {
		return false;
	}
	// Order-dependence gate (mirrors the one-sided IsSupportedAggregate): reassociating the aggregate below the join
	// changes the combine order, so only order-independent or precision-only (sum/avg over DOUBLE) aggregates qualify.
	if (aggr.Function().GetOrderDependent() == AggregateOrderDependent::ORDER_DEPENDENT &&
	    !aggr.Function().GetReassociationPrecisionOnly()) {
		return false;
	}
	auto &name = aggr.Function().GetName();
	if (aggr.GetChildren().empty()) {
		if (name == "count_star") {
			kind = DoubleEagerKind::COUNT_STAR;
			return true;
		}
		return false;
	}
	if (aggr.GetChildren().size() != 1) {
		return false;
	}
	if (name == "sum") {
		kind = DoubleEagerKind::SUM;
	} else if (name == "count") {
		kind = DoubleEagerKind::COUNT;
	} else if (name == "min") {
		kind = DoubleEagerKind::MIN;
	} else if (name == "max") {
		kind = DoubleEagerKind::MAX;
	} else {
		return false;
	}
	return true;
}

struct DoubleEagerHeuristics {
	// Minimum rows-per-join-key a side must have for pre-aggregating it to be worth the extra aggregate
	// (pre-aggregation must at least halve the side). Below this, double-eager is not beneficial.
	static constexpr idx_t MIN_COLLAPSE = 2;
};

// Decide whether double-eager pays off, using only the cardinalities the join-order optimizer already filled
// in. For an equi-join, join_card ~ c0*c1/ndv, so the effective number of distinct join keys is
// ndv ~ c0*c1/join_card, and each side's collapse ratio (rows per key) is c_s/ndv. Double-eager is worth it
// only when BOTH sides collapse by at least MIN_COLLAPSE; when only one side collapses the one-sided path is
// the better fit, and when neither does there is nothing to gain. On success `effective_ndv` carries the
// estimated distinct-key count so the rewritten operators can be given realistic cardinalities (a side
// collapses to ~one row per key) instead of inheriting the pre-aggregation row counts.
static bool DEEstimateCollapse(const LogicalComparisonJoin &join, idx_t &effective_ndv) {
	if (!join.has_estimated_cardinality || !join.children[0]->has_estimated_cardinality ||
	    !join.children[1]->has_estimated_cardinality) {
		return false;
	}
	const auto c0 = static_cast<double>(MaxValue<idx_t>(join.children[0]->estimated_cardinality, 1));
	const auto c1 = static_cast<double>(MaxValue<idx_t>(join.children[1]->estimated_cardinality, 1));
	const auto cj = static_cast<double>(MaxValue<idx_t>(join.estimated_cardinality, 1));
	double ndv = MaxValue<double>(1.0, (c0 * c1) / cj);
	ndv = MinValue<double>(ndv, MinValue<double>(c0, c1));
	effective_ndv = MaxValue<idx_t>(static_cast<idx_t>(ndv), 1);
	return c0 >= static_cast<double>(DoubleEagerHeuristics::MIN_COLLAPSE) * ndv &&
	       c1 >= static_cast<double>(DoubleEagerHeuristics::MIN_COLLAPSE) * ndv;
}

bool PartialAggregatePushdown::TryDoubleEagerPushdown(unique_ptr<LogicalOperator> &op) {
	LogicalAggregate *aggr_ptr;
	LogicalComparisonJoin *join_ptr;
	if (!GetPushdownOperators(*op, aggr_ptr, join_ptr)) {
		return false;
	}
	auto &aggr = *aggr_ptr;
	auto &join = *join_ptr;
	if (aggr.grouping_sets.size() > 1) {
		return false; // ROLLUP / GROUPING SETS not handled by this path
	}
	idx_t effective_ndv;
	if (!DEEstimateCollapse(join, effective_ndv)) {
		return false;
	}

	unordered_set<TableIndex> side_bindings[2];
	LogicalJoin::GetTableReferences(*join.children[0], side_bindings[0]);
	LogicalJoin::GetTableReferences(*join.children[1], side_bindings[1]);
	auto side_of = [&](const Expression &expr, idx_t &side) {
		return GetExpressionSide(expr, side_bindings, side);
	};

	// classify aggregates
	vector<DoubleEagerAggregate> aggregates;
	for (auto &expr : aggr.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		auto &bound = expr->Cast<BoundAggregateExpression>();
		DoubleEagerKind kind;
		if (!DEClassify(bound, kind)) {
			return false;
		}
		DoubleEagerAggregate de;
		de.kind = kind;
		de.return_type = bound.GetReturnType();
		if (kind != DoubleEagerKind::COUNT_STAR) {
			if (!GetColumnBinding(*bound.GetChildren()[0], de.child_binding)) {
				return false;
			}
			if (!side_of(*bound.GetChildren()[0], de.side)) {
				return false;
			}
			de.child_type = bound.GetChildren()[0]->GetReturnType();
			// Double-eager reconstructs sum as `cnt_other * partial`. For an input type with no headroom above
			// its own sum (HUGEINT, or an INT128-backed DECIMAL), that scalar multiply can overflow on a single
			// key even when the row-by-row total fits via sign cancellation - turning a valid query into an
			// error. The one-sided path (additive combine) does not have this hazard, so leave these to it.
			// Narrower DECIMALs (internal type <= INT64, i.e. width <= 18) are safe: sum widens them to
			// DECIMAL(38,s), so cnt_other*partial only overflows once a single key's fan-out exceeds ~1e20 rows
			// (a value <= 1e(18-s) needs that many to approach the 1e(38-s) cap) - not physically reachable.
			if (kind == DoubleEagerKind::SUM &&
			    (de.child_type.id() == LogicalTypeId::HUGEINT || de.child_type.id() == LogicalTypeId::UHUGEINT ||
			     (de.child_type.id() == LogicalTypeId::DECIMAL &&
			      de.child_type.InternalType() == PhysicalType::INT128))) {
				return false;
			}
		}
		aggregates.push_back(std::move(de));
	}

	// validate join conditions: equality between a plain column on each side; collect per-side join keys
	vector<ColumnBinding> join_keys[2];
	vector<LogicalType> join_key_types[2];
	for (auto &cond : join.conditions) {
		if (!cond.IsComparison() || cond.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		idx_t lhs_side, rhs_side;
		if (!side_of(cond.GetLHS(), lhs_side) || !side_of(cond.GetRHS(), rhs_side) || lhs_side == rhs_side) {
			return false;
		}
		auto &lhs_expr = lhs_side == 0 ? cond.GetLHS() : cond.GetRHS();
		auto &rhs_expr = lhs_side == 0 ? cond.GetRHS() : cond.GetLHS();
		ColumnBinding lkey, rkey;
		if (!GetColumnBinding(lhs_expr, lkey) || !GetColumnBinding(rhs_expr, rkey)) {
			return false;
		}
		join_keys[0].push_back(lkey);
		join_key_types[0].push_back(lhs_expr.GetReturnType());
		join_keys[1].push_back(rkey);
		join_key_types[1].push_back(rhs_expr.GetReturnType());
	}

	// validate groups: each a plain column on a definite side
	for (auto &group : aggr.groups) {
		ColumnBinding b;
		idx_t s;
		if (!GetColumnBinding(*group, b) || !side_of(*group, s)) {
			return false;
		}
	}

	// build the two lower aggregates
	DoubleEagerSide sides[2];
	for (idx_t s = 0; s < 2; s++) {
		sides[s].group_index = optimizer.binder.GenerateTableIndex();
		sides[s].aggregate_index = optimizer.binder.GenerateTableIndex();
		CollectLowerSideGroups(sides[s].group_index, join_keys[s], join_key_types[s], aggr.groups, s, side_bindings,
		                       sides[s].groups, sides[s].group_map, sides[s].group_types);
		// position 0 of every lower aggregate is count_star (the per-key multiplicity of this side)
		auto count_star = DEBindAggregate(optimizer.context, "count_star", {});
		if (!count_star) {
			return false;
		}
		sides[s].aggregates.push_back(std::move(count_star));
	}

	// per-side partials for each measured aggregate
	for (auto &de : aggregates) {
		if (de.kind == DoubleEagerKind::COUNT_STAR) {
			continue;
		}
		string fname = de.kind == DoubleEagerKind::SUM     ? "sum"
		               : de.kind == DoubleEagerKind::COUNT ? "count"
		               : de.kind == DoubleEagerKind::MIN   ? "min"
		                                                   : "max";
		vector<unique_ptr<Expression>> child;
		child.push_back(make_uniq<BoundColumnRefExpression>(de.child_type, de.child_binding));
		auto partial = DEBindAggregate(optimizer.context, fname, std::move(child));
		if (!partial) {
			return false;
		}
		de.partial_pos = sides[de.side].aggregates.size();
		sides[de.side].aggregates.push_back(std::move(partial));
	}

	// materialize the lower aggregates on top of each join input
	unique_ptr<LogicalAggregate> lower[2];
	for (idx_t s = 0; s < 2; s++) {
		lower[s] =
		    make_uniq<LogicalAggregate>(sides[s].group_index, sides[s].aggregate_index, std::move(sides[s].aggregates));
		lower[s]->groups = std::move(sides[s].groups);
		lower[s]->children.push_back(std::move(join.children[s]));
		lower[s]->ResolveOperatorTypes();
		// each side collapses to ~one row per distinct join key; reflect that so downstream passes
		// (e.g. build-side selection) see the post-aggregation size, not the raw input size
		lower[s]->SetEstimatedCardinality(effective_ndv);
	}
	// count_star is expression 0 of each lower aggregate: binding is (aggregate_index, 0); its type lives in the
	// operator's `types` vector after the group columns (groups occupy the first lower_group_count slots).
	ColumnBinding cnt_binding[2];
	LogicalType cnt_type[2];
	idx_t lower_group_count[2];
	for (idx_t s = 0; s < 2; s++) {
		lower_group_count[s] = lower[s]->groups.size();
		cnt_binding[s] = ColumnBinding(sides[s].aggregate_index, ProjectionIndex(0));
		cnt_type[s] = lower[s]->types[lower_group_count[s]];
	}

	// rebuild the join over the two lower aggregates
	auto new_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	auto lower_key_expr = [&](idx_t s, idx_t k) {
		auto binding = sides[s].group_map.at(join_keys[s][k]);
		return make_uniq<BoundColumnRefExpression>(join_key_types[s][k], binding);
	};
	for (idx_t k = 0; k < join_keys[0].size(); k++) {
		new_join->conditions.emplace_back(lower_key_expr(0, k), lower_key_expr(1, k), ExpressionType::COMPARE_EQUAL);
	}
	new_join->children.push_back(std::move(lower[0]));
	new_join->children.push_back(std::move(lower[1]));
	new_join->ResolveOperatorTypes();
	// both inputs are now ~one row per key, so the join produces ~one row per shared key
	new_join->SetEstimatedCardinality(effective_ndv);

	// reconstruct each aggregate above the join
	auto partial_ref = [&](const DoubleEagerAggregate &de) {
		auto binding = ColumnBinding(sides[de.side].aggregate_index, ProjectionIndex(de.partial_pos));
		auto type = new_join->children[de.side]->types[lower_group_count[de.side] + de.partial_pos];
		return make_uniq<BoundColumnRefExpression>(type, binding);
	};
	auto cnt_ref = [&](idx_t s) -> unique_ptr<Expression> {
		// scale in HUGEINT: count(*)/count reconstruction multiplies two counts (or a count by a partial), and a
		// BIGINT*BIGINT product would overflow at ~3e9 rows per key on each side even when the true value fits
		unique_ptr<Expression> ref = make_uniq<BoundColumnRefExpression>(cnt_type[s], cnt_binding[s]);
		return BoundCastExpression::AddCastToType(optimizer.context, std::move(ref), LogicalType::HUGEINT);
	};
	vector<unique_ptr<Expression>> upper_aggregates;
	for (auto &de : aggregates) {
		unique_ptr<Expression> upper;
		switch (de.kind) {
		case DoubleEagerKind::COUNT_STAR: {
			auto product = optimizer.BindScalarFunction("*", cnt_ref(0), cnt_ref(1));
			vector<unique_ptr<Expression>> arg;
			arg.push_back(std::move(product));
			upper = DEBindAggregate(optimizer.context, "sum", std::move(arg));
			break;
		}
		case DoubleEagerKind::SUM:
		case DoubleEagerKind::COUNT: {
			auto product = optimizer.BindScalarFunction("*", cnt_ref(1 - de.side), partial_ref(de));
			vector<unique_ptr<Expression>> arg;
			arg.push_back(std::move(product));
			upper = DEBindAggregate(optimizer.context, "sum", std::move(arg));
			break;
		}
		case DoubleEagerKind::MIN:
		case DoubleEagerKind::MAX: {
			// min/max ignore multiplicity: min(min) / max(max) over the per-key partials
			vector<unique_ptr<Expression>> arg;
			arg.push_back(partial_ref(de));
			upper = DEBindAggregate(optimizer.context, de.kind == DoubleEagerKind::MIN ? "min" : "max", std::move(arg));
			break;
		}
		}
		if (!upper) {
			return false;
		}
		upper_aggregates.push_back(std::move(upper));
	}

	// upper aggregate: group by the original group columns (now sourced from the lower aggregates)
	auto upper_group_index = optimizer.binder.GenerateTableIndex();
	auto upper_aggregate_index = optimizer.binder.GenerateTableIndex();
	auto upper_aggr =
	    make_uniq<LogicalAggregate>(upper_group_index, upper_aggregate_index, std::move(upper_aggregates));
	for (auto &group : aggr.groups) {
		auto &ref = group->Cast<BoundColumnRefExpression>();
		idx_t gs;
		side_of(*group, gs);
		auto binding = sides[gs].group_map.at(ref.Binding());
		upper_aggr->groups.push_back(make_uniq<BoundColumnRefExpression>(ref.GetReturnType(), binding));
	}
	upper_aggr->grouping_sets = aggr.grouping_sets;
	upper_aggr->children.push_back(std::move(new_join));
	upper_aggr->ResolveOperatorTypes();
	CopyCardinality(*upper_aggr, aggr);

	// final projection: cast each reconstructed aggregate back to the original return type and remap bindings
	auto projection = BuildFinalProjection(optimizer, aggr, std::move(upper_aggr), replacement_map,
	                                       [&](idx_t j, unique_ptr<Expression> ref) -> unique_ptr<Expression> {
		                                       if (ref->GetReturnType() != aggregates[j].return_type) {
			                                       return BoundCastExpression::AddCastToType(
			                                           optimizer.context, std::move(ref), aggregates[j].return_type);
		                                       }
		                                       return ref;
	                                       });
	if (!projection) {
		return false;
	}
	op = std::move(projection);
	return true;
}

// Replace references to `proj`'s output columns with copies of `proj`'s defining expressions, recursively.
static void DEInlineProjection(unique_ptr<Expression> &expr, const LogicalProjection &proj) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &ref = expr->Cast<BoundColumnRefExpression>();
		if (ref.Binding().table_index == proj.table_index) {
			expr = proj.expressions[ref.Binding().column_index.GetIndex()]->Copy();
			return;
		}
	}
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { DEInlineProjection(child, proj); });
}

bool PartialAggregatePushdown::FuseInterveningProjections(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || op.children.size() != 1) {
		return false;
	}
	if (op.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	// collect the chain of single-child projections and require it to bottom out at a comparison join
	vector<reference<LogicalProjection>> projections;
	reference<LogicalOperator> cur = *op.children[0];
	while (cur.get().type == LogicalOperatorType::LOGICAL_PROJECTION && cur.get().children.size() == 1) {
		projections.push_back(cur.get().Cast<LogicalProjection>());
		cur = *cur.get().children[0];
	}
	if (cur.get().type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	// inlining duplicates a projection expression wherever the aggregate references it, so a volatile
	// expression (e.g. random()) referenced more than once would be evaluated independently - bail on those
	for (auto &proj : projections) {
		for (auto &expr : proj.get().expressions) {
			if (expr->IsVolatile()) {
				return false;
			}
		}
	}
	auto &aggr = op.Cast<LogicalAggregate>();
	// inline the projections (top-to-bottom) into the aggregate's groups and aggregate expressions, so they
	// reference the join's output columns directly
	auto inline_all = [&](unique_ptr<Expression> &e) {
		for (auto &proj : projections) {
			DEInlineProjection(e, proj.get());
		}
	};
	for (auto &group : aggr.groups) {
		inline_all(group);
	}
	for (auto &expr : aggr.expressions) {
		inline_all(expr);
	}
	// splice the join in as the aggregate's direct child, dropping the now-absorbed projections
	op.children[0] = std::move(projections.back().get().children[0]);
	return true;
}

void PartialAggregatePushdown::VisitOperator(unique_ptr<LogicalOperator> &op) {
	LogicalOperatorVisitor::VisitOperator(op);
	FuseInterveningProjections(*op);
	if (TryDoubleEagerPushdown(op) || TryPushdownAggregate(op)) {
		// The rewrite introduces new aggregate-over-join nodes (the per-side lower aggregates). Re-traverse the
		// rewritten subtree so any lower aggregate that sits over a join gets pushed in turn - this collapses
		// every fact in a multi-fact chain, not just the two inputs of the topmost join (N-way pushdown). The
		// re-traversal also runs VisitReplace over the join/aggregate we just built, applying the binding
		// rewrites produced by those recursive pushdowns. Aggregate inputs (the lower aggregates) block any
		// further pushdown of the upper aggregate, so the recursion only deepens and terminates at base scans.
		LogicalOperatorVisitor::VisitOperator(op);
	}
}

unique_ptr<Expression> PartialAggregatePushdown::VisitReplace(BoundColumnRefExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	auto entry = replacement_map.find(expr.Binding());
	if (entry != replacement_map.end()) {
		expr.BindingMutable() = entry->second;
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
	info.upper_group_index = optimizer.binder.GenerateTableIndex();
	info.upper_aggregate_index = optimizer.binder.GenerateTableIndex();
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
	auto final_projection =
	    BuildFinalProjection(optimizer, *aggr, std::move(upper_aggr), replacement_map,
	                         [&](idx_t j, unique_ptr<Expression> ref) -> unique_ptr<Expression> {
		                         auto finalized = optimizer.BindScalarFunction("finalize", std::move(ref));
		                         if (finalized->GetReturnType() != aggr->expressions[j]->GetReturnType()) {
			                         return nullptr; // state does not round-trip back to the original type
		                         }
		                         return finalized;
	                         });
	if (!final_projection) {
		return false;
	}
	op = std::move(final_projection);
	return true;
}

} // namespace duckdb
