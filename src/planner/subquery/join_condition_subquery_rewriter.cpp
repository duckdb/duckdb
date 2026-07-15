#include "duckdb/function/window/rows_functions.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/lateral_binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/logical_operator_deep_copy.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/subquery/column_binding_layout.hpp"
#include "duckdb/planner/subquery/recursive_dependent_join_planner.hpp"

#include <algorithm>

namespace duckdb {

// Join-condition subqueries that reference both inputs cannot be planned against
// one child. Non-FULL joins are lowered to the existing dependent/lateral join
// representation. FULL OUTER joins need the paper's match-domain rewrite so
// unmatched rows from both inputs can be reconstructed after the predicate has
// been decorrelated by the normal RecursiveDependentJoinPlanner/FlattenDependentJoins path.

static vector<Identifier> GenerateInternalColumnNames(idx_t column_count, const string &prefix) {
	vector<Identifier> result;
	result.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result.push_back(Identifier(prefix + to_string(i)));
	}
	return result;
}

static void AddPairDependentFilterToLateralChild(unique_ptr<LogicalOperator> &lateral_child,
                                                 unique_ptr<Expression> condition) {
	auto filter = make_uniq<LogicalFilter>(std::move(condition));
	if (lateral_child->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE ||
	    lateral_child->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte = lateral_child->Cast<LogicalCTE>();
		filter->AddChild(std::move(cte.children[1]));
		cte.children[1] = std::move(filter);
		return;
	}
	filter->AddChild(std::move(lateral_child));
	lateral_child = std::move(filter);
}

unique_ptr<LogicalOperator> RecursiveDependentJoinPlanner::PlanPairDependentLateralJoin(
    Binder &binder, unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
    unique_ptr<Expression> condition, const unordered_set<TableIndex> &left_bindings,
    const unordered_set<TableIndex> &right_bindings, JoinType join_type) {
	CorrelatedColumns correlated_columns;
	if (!LateralBinder::ExtractPairDependentJoinConditionCorrelations(*right, condition, left_bindings, right_bindings,
	                                                                  correlated_columns)) {
		throw InternalException("Pair-dependent join condition did not produce lateral correlations");
	}
	AddPairDependentFilterToLateralChild(right, std::move(condition));
	auto planned = binder.PlanLateralJoin(std::move(left), std::move(right), correlated_columns, join_type, nullptr);
	if (!planned) {
		throw InternalException("Failed to plan pair-dependent join condition");
	}
	return planned;
}

// FULL OUTER pair-dependent predicates are planned through a match-domain CTE. The match domain evaluates the
// original predicate over generated CTE refs for both sides, then the outer join is reconstructed from the distinct
// matched side payloads plus unmatched rows from each original side.

static unique_ptr<LogicalWindow> CreatePairRowNumberWindow(Binder &binder, unique_ptr<LogicalOperator> child,
                                                           TableIndex table_index) {
	auto window = make_uniq<LogicalWindow>(table_index);

	auto row_number = RowNumberFun::GetFunction().Bind(binder.context);
	row_number->WindowStartMutable() = WindowBoundary::UNBOUNDED_PRECEDING;
	row_number->WindowEndMutable() = WindowBoundary::CURRENT_ROW_ROWS;
	row_number->SetAlias("__duckdb_pair_rowid");

	window->expressions.push_back(std::move(row_number));
	window->AddChild(std::move(child));
	return window;
}

struct BoundColumnPayload {
	BoundColumnPayload() = default;

	BoundColumnPayload(const vector<LogicalType> &types, const vector<ColumnBinding> &source_bindings) : types(types) {
		D_ASSERT(source_bindings.size() >= types.size());
		bindings.reserve(types.size());
		for (idx_t i = 0; i < types.size(); i++) {
			bindings.push_back(source_bindings[i]);
		}
	}

	idx_t ColumnCount() const {
		return bindings.size();
	}

	BoundColumnPayload Slice(idx_t offset, idx_t count) const {
		D_ASSERT(offset + count <= types.size());
		D_ASSERT(offset + count <= bindings.size());
		BoundColumnPayload result;
		result.types.reserve(count);
		result.bindings.reserve(count);
		for (idx_t i = 0; i < count; i++) {
			result.types.push_back(types[offset + i]);
			result.bindings.push_back(bindings[offset + i]);
		}
		return result;
	}

	void Append(const BoundColumnPayload &other) {
		types.insert(types.end(), other.types.begin(), other.types.end());
		bindings.insert(bindings.end(), other.bindings.begin(), other.bindings.end());
	}

	vector<LogicalType> types;
	vector<ColumnBinding> bindings;
};

struct SideBindingLayout {
	TableIndex table_index;
	vector<idx_t> payload_indices;
	vector<ProjectionIndex> column_indices;
	idx_t column_count = 0;
	ProjectionIndex row_id_column_index;
	TableIndex cte_index;
	vector<LogicalType> cte_types;
	vector<Identifier> names;
	unique_ptr<LogicalOperator> cte_source;
};

struct PairDependentJoinSide {
	TableIndex cte_index;
	vector<ColumnBinding> bindings;
	vector<LogicalType> types;
	vector<LogicalType> cte_types;
	vector<Identifier> names;
	vector<SideBindingLayout> binding_layouts;
	bool has_row_id = false;
	idx_t row_id_offset = DConstants::INVALID_INDEX;
};

struct PairDependentJoinMatch {
	TableIndex cte_index;
	vector<LogicalType> types;
	vector<Identifier> names;
};

struct PairDependentSideRef {
	unique_ptr<LogicalOperator> plan;
	BoundColumnPayload payload;
	ColumnBindingLayout output;
};

struct PairDependentSideLayoutRef {
	unique_ptr<LogicalOperator> plan;
	BoundColumnPayload payload;
	ColumnBinding row_id_binding;
	ColumnBindingLayout output;
};

class PairDependentFullOuterJoinBuilder {
public:
	PairDependentFullOuterJoinBuilder(Binder &binder, unique_ptr<Expression> condition,
	                                  unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right)
	    : binder(binder), condition(std::move(condition)), left(std::move(left)), right(std::move(right)) {
	}

	unique_ptr<LogicalOperator> Build();

private:
	bool CreateMatchSet();
	unique_ptr<LogicalOperator> CreateFullJoin();
	unique_ptr<LogicalOperator> WrapCTEs(unique_ptr<LogicalOperator> final_join);

private:
	Binder &binder;
	unique_ptr<Expression> condition;
	unique_ptr<LogicalOperator> left;
	unique_ptr<LogicalOperator> right;
	PairDependentJoinSide left_side;
	PairDependentJoinSide right_side;
	PairDependentJoinMatch match;
	unique_ptr<LogicalOperator> match_source;
};

static SideBindingLayout &GetOrCreateBindingLayout(vector<SideBindingLayout> &layouts, TableIndex table_index) {
	for (auto &layout : layouts) {
		if (layout.table_index == table_index) {
			return layout;
		}
	}
	SideBindingLayout layout;
	layout.table_index = table_index;
	layouts.push_back(std::move(layout));
	return layouts.back();
}

static vector<SideBindingLayout> BuildSideBindingLayouts(const vector<ColumnBinding> &bindings) {
	vector<SideBindingLayout> layouts;
	for (idx_t payload_index = 0; payload_index < bindings.size(); payload_index++) {
		auto &binding = bindings[payload_index];
		auto &layout = GetOrCreateBindingLayout(layouts, binding.table_index);
		layout.payload_indices.push_back(payload_index);
		layout.column_indices.push_back(binding.column_index);
		layout.column_count = std::max(layout.column_count, binding.column_index.GetIndex() + 1);
	}
	for (auto &layout : layouts) {
		layout.row_id_column_index = ProjectionIndex(layout.column_count);
	}
	return layouts;
}

static void SetJoinProjectionMaps(LogicalOperator &join, const ColumnBindingLayout &left_output,
                                  const vector<ColumnBinding> &selected_left_bindings,
                                  const ColumnBindingLayout &right_output,
                                  const vector<ColumnBinding> &selected_right_bindings) {
	auto &logical_join = join.Cast<LogicalJoin>();
	if (selected_left_bindings.empty()) {
		throw InternalException("Cannot create an empty left projection map for a pair-dependent join");
	}
	if (selected_right_bindings.empty()) {
		throw InternalException("Cannot create an empty right projection map for a pair-dependent join");
	}
	logical_join.left_projection_map = left_output.CreateProjectionMap(selected_left_bindings);
	logical_join.right_projection_map = right_output.CreateProjectionMap(selected_right_bindings);
}

static void AddNotDistinctConditions(vector<JoinCondition> &conditions, const BoundColumnPayload &left_payload,
                                     const BoundColumnPayload &right_payload) {
	D_ASSERT(left_payload.ColumnCount() == right_payload.ColumnCount());
	for (idx_t i = 0; i < left_payload.ColumnCount(); i++) {
		auto left = make_uniq<BoundColumnRefExpression>(left_payload.types[i], left_payload.bindings[i]);
		auto right = make_uniq<BoundColumnRefExpression>(right_payload.types[i], right_payload.bindings[i]);
		conditions.emplace_back(std::move(left), std::move(right), ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	}
}

static bool PreparePairDependentJoinSide(Binder &binder, unique_ptr<LogicalOperator> &side, PairDependentJoinSide &info,
                                         const string &name_prefix) {
	side->ResolveOperatorTypes();

	info.bindings = side->GetColumnBindings();
	info.types = side->types;
	D_ASSERT(info.bindings.size() == info.types.size());
	if (info.bindings.empty()) {
		return false;
	}
	info.binding_layouts = BuildSideBindingLayouts(info.bindings);
	info.has_row_id = info.binding_layouts.size() > 1;
	if (info.has_row_id) {
		// Multiple binding layouts are stitched back together after reading the side CTE.
		// The row number is part of that CTE source, so all layout refs share the same identity.
		info.row_id_offset = info.types.size();
		side = CreatePairRowNumberWindow(binder, std::move(side), binder.GenerateTableIndex());
		side->ResolveOperatorTypes();
		D_ASSERT(side->types.size() == info.types.size() + 1);
	}
	info.cte_types = side->types;
	info.names = GenerateInternalColumnNames(info.cte_types.size(), name_prefix);
	return true;
}

static unique_ptr<LogicalOperator> CreateDistinctMatchProjection(Binder &binder, unique_ptr<LogicalOperator> match,
                                                                 const BoundColumnPayload &payload,
                                                                 vector<LogicalType> &match_types) {
	match->ResolveOperatorTypes();
	auto match_bindings = match->GetColumnBindings();

	auto group_index = binder.GenerateTableIndex();
	auto aggregate_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> aggregates;
	auto distinct = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
	for (idx_t i = 0; i < payload.bindings.size(); i++) {
		D_ASSERT(std::find(match_bindings.begin(), match_bindings.end(), payload.bindings[i]) != match_bindings.end());
		distinct->groups.push_back(make_uniq<BoundColumnRefExpression>(payload.types[i], payload.bindings[i]));
	}
	distinct->children.push_back(std::move(match));
	distinct->ResolveOperatorTypes();

	auto distinct_bindings = distinct->GetColumnBindings();
	vector<unique_ptr<Expression>> projections;
	projections.reserve(distinct_bindings.size() + 1);
	for (idx_t i = 0; i < distinct_bindings.size(); i++) {
		projections.push_back(make_uniq<BoundColumnRefExpression>(distinct->types[i], distinct_bindings[i]));
	}
	projections.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(projections));
	projection->children.push_back(std::move(distinct));
	projection->ResolveOperatorTypes();
	match_types = projection->types;
	return projection;
}

static bool CanUseDirectSideCTERef(const PairDependentJoinSide &side) {
	// A single contiguous binding layout can read the side CTE directly using
	// the original table index. Extra layout CTEs are only needed when one side
	// exposes multiple table-index layouts that must be stitched back together.
	if (side.has_row_id || side.binding_layouts.size() != 1) {
		return false;
	}
	for (idx_t i = 0; i < side.bindings.size(); i++) {
		if (side.bindings[i] != ColumnBinding(side.binding_layouts[0].table_index, ProjectionIndex(i))) {
			return false;
		}
	}
	return true;
}

static unique_ptr<LogicalOperator> CreateSideBindingLayoutSource(Binder &binder, const PairDependentJoinSide &side,
                                                                 const SideBindingLayout &layout, bool include_row_id) {
	auto cte_ref = make_uniq<LogicalCTERef>(binder.GenerateTableIndex(), side.cte_index, side.cte_types, side.names);
	auto source_bindings = cte_ref->GetColumnBindings();
	auto expression_count = layout.column_count + (include_row_id ? 1 : 0);

	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(expression_count);
	for (idx_t i = 0; i < expression_count; i++) {
		expressions.push_back(make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}
	for (idx_t layout_idx = 0; layout_idx < layout.payload_indices.size(); layout_idx++) {
		auto payload_index = layout.payload_indices[layout_idx];
		auto column_index = layout.column_indices[layout_idx];
		D_ASSERT(column_index.GetIndex() < expressions.size());
		expressions[column_index.GetIndex()] =
		    make_uniq<BoundColumnRefExpression>(side.types[payload_index], source_bindings[payload_index]);
	}
	if (include_row_id) {
		D_ASSERT(side.has_row_id);
		D_ASSERT(side.row_id_offset < side.cte_types.size());
		D_ASSERT(layout.row_id_column_index.GetIndex() < expressions.size());
		expressions[layout.row_id_column_index.GetIndex()] = make_uniq<BoundColumnRefExpression>(
		    side.cte_types[side.row_id_offset], source_bindings[side.row_id_offset]);
	}

	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(expressions));
	projection->children.push_back(std::move(cte_ref));
	projection->ResolveOperatorTypes();
	return projection;
}

static void CreateSideBindingLayoutCTEs(Binder &binder, PairDependentJoinSide &side, const string &name_prefix) {
	if (CanUseDirectSideCTERef(side)) {
		return;
	}
	for (idx_t layout_idx = 0; layout_idx < side.binding_layouts.size(); layout_idx++) {
		auto &layout = side.binding_layouts[layout_idx];
		layout.cte_index = binder.GenerateTableIndex();
		layout.cte_source = CreateSideBindingLayoutSource(binder, side, layout, side.has_row_id);
		layout.cte_types = layout.cte_source->types;
		layout.names = GenerateInternalColumnNames(layout.cte_types.size(), name_prefix);
	}
}

static PairDependentSideLayoutRef CreateSideBindingLayoutRef(const PairDependentJoinSide &side,
                                                             const SideBindingLayout &layout, bool include_row_id) {
	D_ASSERT(layout.cte_source);
	auto cte_ref = make_uniq<LogicalCTERef>(layout.table_index, layout.cte_index, layout.cte_types, layout.names);
	auto source_bindings = cte_ref->GetColumnBindings();

	PairDependentSideLayoutRef result;
	result.payload.types.reserve(layout.payload_indices.size());
	result.payload.bindings.reserve(layout.payload_indices.size());
	for (idx_t i = 0; i < layout.payload_indices.size(); i++) {
		result.payload.types.push_back(side.types[layout.payload_indices[i]]);
		auto column_index = layout.column_indices[i].GetIndex();
		D_ASSERT(column_index < source_bindings.size());
		result.payload.bindings.push_back(source_bindings[column_index]);
	}
	if (include_row_id) {
		auto row_id_index = layout.row_id_column_index.GetIndex();
		D_ASSERT(row_id_index < source_bindings.size());
		result.row_id_binding = source_bindings[row_id_index];
	}
	result.plan = std::move(cte_ref);
	result.output = ColumnBindingLayout(result.plan->GetColumnBindings(), "pair-dependent join side");
	return result;
}

static PairDependentSideRef CreatePairDependentSideRef(const PairDependentJoinSide &side) {
	if (CanUseDirectSideCTERef(side)) {
		auto plan =
		    make_uniq<LogicalCTERef>(side.binding_layouts[0].table_index, side.cte_index, side.cte_types, side.names);
		auto output = ColumnBindingLayout(plan->GetColumnBindings(), "pair-dependent join side");
		return PairDependentSideRef {std::move(plan), BoundColumnPayload(side.types, side.bindings), std::move(output)};
	}

	if (!side.has_row_id) {
		D_ASSERT(side.binding_layouts.size() == 1);
		auto layout_ref = CreateSideBindingLayoutRef(side, side.binding_layouts[0], false);
		return PairDependentSideRef {std::move(layout_ref.plan), std::move(layout_ref.payload),
		                             std::move(layout_ref.output)};
	}

	auto current_ref = CreateSideBindingLayoutRef(side, side.binding_layouts[0], true);
	auto current = std::move(current_ref.plan);
	auto current_payload = std::move(current_ref.payload);
	auto current_row_id_binding = current_ref.row_id_binding;
	auto current_output = std::move(current_ref.output);
	auto row_id_type = side.cte_types[side.row_id_offset];

	for (idx_t layout_idx = 1; layout_idx < side.binding_layouts.size(); layout_idx++) {
		auto next_ref = CreateSideBindingLayoutRef(side, side.binding_layouts[layout_idx], true);
		current->ResolveOperatorTypes();
		next_ref.plan->ResolveOperatorTypes();

		vector<JoinCondition> conditions;
		auto left_row_id = make_uniq<BoundColumnRefExpression>(row_id_type, current_row_id_binding);
		auto right_row_id = make_uniq<BoundColumnRefExpression>(row_id_type, next_ref.row_id_binding);
		conditions.emplace_back(std::move(left_row_id), std::move(right_row_id),
		                        ExpressionType::COMPARE_NOT_DISTINCT_FROM);

		// These joins only stitch together different projections of the same side CTE.
		// A match must exist for every row id, and keeping this as a LEFT join prevents
		// filter pullup from lifting the stitching predicate above the public side shape.
		auto joined = LogicalComparisonJoin::CreateJoin(JoinType::LEFT, JoinRefType::REGULAR, std::move(current),
		                                                std::move(next_ref.plan), std::move(conditions));
		auto left_outputs = current_payload.bindings;
		left_outputs.push_back(current_row_id_binding);
		SetJoinProjectionMaps(*joined, current_output, left_outputs, next_ref.output, next_ref.payload.bindings);
		joined->ResolveOperatorTypes();
		current_output = ColumnBindingLayout(joined->GetColumnBindings(), "pair-dependent join side");
		current = std::move(joined);
		current_payload.types.insert(current_payload.types.end(), next_ref.payload.types.begin(),
		                             next_ref.payload.types.end());
		current_payload.bindings.insert(current_payload.bindings.end(), next_ref.payload.bindings.begin(),
		                                next_ref.payload.bindings.end());
	}

	return PairDependentSideRef {std::move(current), BoundColumnPayload(side.types, side.bindings),
	                             std::move(current_output)};
}

unique_ptr<LogicalOperator> PairDependentFullOuterJoinBuilder::CreateFullJoin() {
	auto final_left = CreatePairDependentSideRef(left_side);
	auto final_right = CreatePairDependentSideRef(right_side);
	auto left_match_index = binder.GenerateTableIndex();
	auto left_match = make_uniq<LogicalCTERef>(left_match_index, match.cte_index, match.types, match.names);

	auto left_match_payload = BoundColumnPayload(match.types, left_match->GetColumnBindings());
	auto left_match_output = ColumnBindingLayout(left_match->GetColumnBindings(), "pair-dependent join side");

	vector<JoinCondition> left_match_conditions;
	AddNotDistinctConditions(left_match_conditions, final_left.payload,
	                         left_match_payload.Slice(0, left_side.types.size()));
	auto left_with_matches =
	    LogicalComparisonJoin::CreateJoin(JoinType::LEFT, JoinRefType::REGULAR, std::move(final_left.plan),
	                                      std::move(left_match), std::move(left_match_conditions));
	SetJoinProjectionMaps(*left_with_matches, final_left.output, left_side.bindings, left_match_output,
	                      left_match_payload.bindings);
	auto left_with_matches_output =
	    ColumnBindingLayout(left_with_matches->GetColumnBindings(), "pair-dependent join side");

	auto marker_binding = left_match_payload.bindings[match.types.size() - 1];

	vector<JoinCondition> final_conditions;
	AddNotDistinctConditions(final_conditions,
	                         left_match_payload.Slice(left_side.types.size(), right_side.types.size()),
	                         final_right.payload);
	auto marker_ref = make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, marker_binding);
	auto marker_true = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	final_conditions.emplace_back(BoundComparisonExpression::Create(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
	                                                                std::move(marker_ref), std::move(marker_true)));

	auto final_join =
	    LogicalComparisonJoin::CreateJoin(JoinType::OUTER, JoinRefType::REGULAR, std::move(left_with_matches),
	                                      std::move(final_right.plan), std::move(final_conditions));
	auto expected_bindings = left_side.bindings;
	expected_bindings.insert(expected_bindings.end(), right_side.bindings.begin(), right_side.bindings.end());
	SetJoinProjectionMaps(*final_join, left_with_matches_output, left_side.bindings, final_right.output,
	                      right_side.bindings);
	final_join->ResolveOperatorTypes();
	auto actual_bindings = final_join->GetColumnBindings();
	D_ASSERT(actual_bindings == expected_bindings);
	return final_join;
}

static unique_ptr<LogicalOperator> WrapSideBindingLayoutCTEs(PairDependentJoinSide &side,
                                                             unique_ptr<LogicalOperator> result) {
	for (idx_t layout_idx = side.binding_layouts.size(); layout_idx > 0; layout_idx--) {
		auto &layout = side.binding_layouts[layout_idx - 1];
		if (!layout.cte_source) {
			continue;
		}
		auto cte_name = Identifier("__duckdb_pair_layout_" + to_string(layout.cte_index.index));
		result = make_uniq<LogicalMaterializedCTE>(cte_name, layout.cte_index, layout.cte_types.size(),
		                                           std::move(layout.cte_source), std::move(result),
		                                           CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	}
	return result;
}

unique_ptr<LogicalOperator> PairDependentFullOuterJoinBuilder::WrapCTEs(unique_ptr<LogicalOperator> final_join) {
	auto match_cte_index = match.cte_index;
	auto match_cte_name = Identifier("__duckdb_pair_match_" + to_string(match_cte_index.index));
	unique_ptr<LogicalOperator> result =
	    make_uniq<LogicalMaterializedCTE>(match_cte_name, match_cte_index, match.types.size(), std::move(match_source),
	                                      std::move(final_join), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	result = WrapSideBindingLayoutCTEs(right_side, std::move(result));
	result = WrapSideBindingLayoutCTEs(left_side, std::move(result));
	auto right_cte_index = right_side.cte_index;
	auto right_cte_name = Identifier("__duckdb_pair_right_" + to_string(right_cte_index.index));
	result =
	    make_uniq<LogicalMaterializedCTE>(right_cte_name, right_cte_index, right_side.cte_types.size(),
	                                      std::move(right), std::move(result), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	auto left_cte_index = left_side.cte_index;
	auto left_cte_name = Identifier("__duckdb_pair_left_" + to_string(left_cte_index.index));
	result =
	    make_uniq<LogicalMaterializedCTE>(left_cte_name, left_cte_index, left_side.cte_types.size(), std::move(left),
	                                      std::move(result), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	return result;
}

bool PairDependentFullOuterJoinBuilder::CreateMatchSet() {
	left->ResolveOperatorTypes();
	right->ResolveOperatorTypes();

	if (!PreparePairDependentJoinSide(binder, left, left_side, "__duckdb_full_l_") ||
	    !PreparePairDependentJoinSide(binder, right, right_side, "__duckdb_full_r_")) {
		return false;
	}

	left_side.cte_index = binder.GenerateTableIndex();
	right_side.cte_index = binder.GenerateTableIndex();
	match.cte_index = binder.GenerateTableIndex();
	CreateSideBindingLayoutCTEs(binder, left_side, "__duckdb_full_l_");
	CreateSideBindingLayoutCTEs(binder, right_side, "__duckdb_full_r_");

	// The original side table indexes become the public bindings of the CTE refs
	// used to reconstruct the join. Remap the CTE definitions themselves so the
	// source plans cannot collide with those public bindings.
	LogicalOperatorDeepCopy left_remapper(binder, nullptr);
	left_remapper.RemapTableIndexesInPlace(*left);
	LogicalOperatorDeepCopy right_remapper(binder, nullptr);
	right_remapper.RemapTableIndexesInPlace(*right);

	auto match_left = make_uniq<LogicalCTERef>(binder.GenerateTableIndex(), left_side.cte_index, left_side.cte_types,
	                                           left_side.names);
	auto match_right = make_uniq<LogicalCTERef>(binder.GenerateTableIndex(), right_side.cte_index, right_side.cte_types,
	                                            right_side.names);
	auto match_left_bindings = match_left->GetColumnBindings();
	auto match_right_bindings = match_right->GetColumnBindings();
	auto match_left_payload = BoundColumnPayload(left_side.types, match_left_bindings);
	auto match_right_payload = BoundColumnPayload(right_side.types, match_right_bindings);
	auto match_payload = match_left_payload;
	match_payload.Append(match_right_payload);

	CorrelatedColumnBindingReplacer replace_condition;
	replace_condition.AddReplacements(left_side.bindings, match_left_payload.bindings);
	replace_condition.AddReplacements(right_side.bindings, match_right_payload.bindings);
	replace_condition.VisitExpression(&condition);

	auto match_join = LogicalCrossProduct::Create(std::move(match_left), std::move(match_right));
	auto match_filter = make_uniq<LogicalFilter>(std::move(condition));
	match_filter->AddChild(std::move(match_join));

	// Match existing decorrelation behavior: volatile predicates are evaluated on the decorrelated match domain,
	// so duplicate-equivalent row pairs can share one predicate result.
	match_source = CreateDistinctMatchProjection(binder, std::move(match_filter), match_payload, match.types);
	match.names = GenerateInternalColumnNames(match.types.size(), "__duckdb_full_match_");
	return true;
}

unique_ptr<LogicalOperator> PairDependentFullOuterJoinBuilder::Build() {
	if (!CreateMatchSet()) {
		throw InternalException("Failed to create pair-dependent FULL OUTER join match set");
	}
	return WrapCTEs(CreateFullJoin());
}

static bool HasPairDependentSubquery(Expression &expression, const unordered_set<TableIndex> &left_bindings,
                                     const unordered_set<TableIndex> &right_bindings) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY &&
	    JoinSide::GetCurrentJoinSide(expression, left_bindings, right_bindings) == JoinSide::BOTH) {
		return true;
	}
	bool result = false;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &child) {
		result = result || HasPairDependentSubquery(child, left_bindings, right_bindings);
	});
	return result;
}

static bool HasPairDependentSubquery(JoinCondition &condition, const unordered_set<TableIndex> &left_bindings,
                                     const unordered_set<TableIndex> &right_bindings) {
	if (condition.IsComparison()) {
		return HasPairDependentSubquery(condition.GetLHS(), left_bindings, right_bindings) ||
		       HasPairDependentSubquery(condition.GetRHS(), left_bindings, right_bindings);
	}
	return HasPairDependentSubquery(condition.GetJoinExpression(), left_bindings, right_bindings);
}

static bool HasPairDependentSubquery(LogicalOperator &op, const unordered_set<TableIndex> &left_bindings,
                                     const unordered_set<TableIndex> &right_bindings) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &condition : join.conditions) {
			if (HasPairDependentSubquery(condition, left_bindings, right_bindings)) {
				return true;
			}
		}
		return false;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		return HasPairDependentSubquery(*op.Cast<LogicalAnyJoin>().condition, left_bindings, right_bindings);
	default:
		return false;
	}
}

static bool SupportsPairDependentRewrite(JoinType join_type) {
	switch (join_type) {
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::OUTER:
		return true;
	default:
		return false;
	}
}

bool RecursiveDependentJoinPlanner::CanRewritePairDependentJoinCondition(LogicalOperator &op) {
	if (op.children.size() != 2 ||
	    (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN && op.type != LogicalOperatorType::LOGICAL_ANY_JOIN)) {
		return false;
	}
	if (!SupportsPairDependentRewrite(op.Cast<LogicalJoin>().join_type)) {
		return false;
	}
	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*op.children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op.children[1], right_bindings);
	return HasPairDependentSubquery(op, left_bindings, right_bindings);
}

static unique_ptr<Expression> MoveJoinCondition(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return JoinCondition::CreateExpression(std::move(op.Cast<LogicalComparisonJoin>().conditions));
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		return std::move(op.Cast<LogicalAnyJoin>().condition);
	default:
		return nullptr;
	}
}

bool RecursiveDependentJoinPlanner::TryRewritePairDependentJoinCondition(Binder &binder,
                                                                         unique_ptr<LogicalOperator> &op) {
	if (!op || !CanRewritePairDependentJoinCondition(*op)) {
		return false;
	}

	auto &join = op->Cast<LogicalJoin>();
	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);
	if (join.join_type == JoinType::OUTER) {
		op->children[0]->ResolveOperatorTypes();
		op->children[1]->ResolveOperatorTypes();
		if (op->children[0]->GetColumnBindings().empty() || op->children[1]->GetColumnBindings().empty()) {
			return false;
		}
	}

	auto condition = MoveJoinCondition(*op);
	auto left = std::move(op->children[0]);
	auto right = std::move(op->children[1]);

	switch (join.join_type) {
	case JoinType::LEFT: {
		op = PlanPairDependentLateralJoin(binder, std::move(left), std::move(right), std::move(condition),
		                                  left_bindings, right_bindings, JoinType::LEFT);
		return true;
	}
	case JoinType::RIGHT: {
		op = PlanPairDependentLateralJoin(binder, std::move(right), std::move(left), std::move(condition),
		                                  right_bindings, left_bindings, JoinType::LEFT);
		return true;
	}
	case JoinType::SEMI:
	case JoinType::ANTI: {
		op = PlanPairDependentLateralJoin(binder, std::move(left), std::move(right), std::move(condition),
		                                  left_bindings, right_bindings, join.join_type);
		return true;
	}
	case JoinType::OUTER: {
		PairDependentFullOuterJoinBuilder builder(binder, std::move(condition), std::move(left), std::move(right));
		op = builder.Build();
		return true;
	}
	default:
		return false;
	}
}

} // namespace duckdb
