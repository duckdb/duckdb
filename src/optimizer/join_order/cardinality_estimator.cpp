#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/optimizer/join_order/join_order_operator.hpp"
#include "duckdb/optimizer/join_order/join_predicate.hpp"
#include "duckdb/optimizer/relation_statistics/equality_cardinality.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

#include <algorithm>

namespace duckdb {

struct DomainEstimate : JoinDomainEstimate {
	CardinalityValue GetDistinctCount(const JoinPredicate &predicate) const {
		auto point = Resolve({predicate.GetLeftStatsBinding(), predicate.GetRightStatsBinding()});
		auto scan = UnknownScanBasis();
		auto comparison = predicate.GetComparisonType();
		if (!point.has_usable_domain && scan && point.distinct_count > 0 &&
		    predicate.GetJoinType() == JoinType::INNER &&
		    (comparison == ExpressionType::COMPARE_EQUAL || comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM)) {
			return MinValue(point.distinct_count,
			                MaxValue(CardinalityValue(1), *scan * RelationStatisticsHelper::DEFAULT_SELECTIVITY));
		}
		return point.distinct_count;
	}
};

struct RelationsSetToStats {
public:
	explicit RelationsSetToStats(const column_binding_set_t &column_binding_set)
	    : equivalent_relations(column_binding_set) {
	}

public:
	//! Column binding sets that are equivalent in a join plan.
	column_binding_set_t equivalent_relations;
	DomainEstimate domain_estimate;
	vector<reference<JoinPredicate>> predicates;
};

struct FilterInfoWithTotalDomains {
public:
	FilterInfoWithTotalDomains(JoinPredicate &predicate, RelationsSetToStats &relation_set_to_stats)
	    : predicate(predicate), distinct_count(relation_set_to_stats.domain_estimate.GetDistinctCount(predicate)),
	      residual(predicate.IsResidualFilter()) {
	}

public:
	CardinalityValue GetDistinctCount() const;
	ExpressionType GetComparisonType();
	FilterInfo &GetFilter() const;
	JoinPredicate &GetPredicate() const;

public:
	reference<JoinPredicate> predicate;
	CardinalityValue distinct_count;
	bool residual;
};

struct LogicalJoinStats {
public:
	LogicalJoinStats(const JoinOrderOperator &op, JoinRelationSet &scope) : op(op), scope(scope) {
	}

public:
	reference<const JoinOrderOperator> op;
	reference<JoinRelationSet> scope;
	vector<pair<ColumnBinding, ColumnBinding>> equality_keys;
	vector<pair<ColumnBinding, ColumnBinding>> value_equalities;
	bool has_residual = false;
};

struct RegionEstimate {
	CardinalityValue cardinality;
	optional_idx maximum;
	CardinalityValue row_retention;
};

struct ResidualFilterStats {
	reference<FilterInfo> filter;
	CardinalityValue selectivity;
	bool proven_empty;
};

static CardinalityValue GetResidualOnSelectivity(const Expression &expression);

struct CardinalityEstimatorState {
public:
	void InitializeEdgeIndex();
	bool TryGetCandidateEdges(const JoinRelationSet &set, vector<idx_t> &candidates) const;

public:
	vector<RelationsSetToStats> relation_set_stats;
	vector<FilterInfoWithTotalDomains> edges;
	//! Each edge is indexed by one of the relations required by its filter.
	vector<vector<idx_t>> edges_by_relation;
	vector<idx_t> unscoped_edges;

	//! The owned leaves outlive the equality model that borrows them.
	vector<RelationStats> leaves;
	unique_ptr<EqualityCardinalityModel> equalities;

	vector<reference<const JoinOrderOperator>> operators;
	vector<LogicalJoinStats> boundaries;

	reference_map_t<JoinRelationSet, CardinalityValue> inner_selectivities;
	reference_map_t<JoinRelationSet, RegionEstimate> cardinalities;
	//! Only requested columns at semantic boundaries; inner subsets do not acquire full column vectors.
	reference_map_t<JoinRelationSet, column_binding_map_t<RelationColumnStats>> projected_columns;

	vector<ResidualFilterStats> residual_filters;
	unordered_set<idx_t> leaf_filters;
};

CardinalityEstimator::CardinalityEstimator(JoinRelationSetManager &set_manager,
                                           const JoinPredicateModel &predicate_model)
    : state(make_uniq<CardinalityEstimatorState>()), set_manager(set_manager), predicate_model(predicate_model) {
}

CardinalityEstimator::~CardinalityEstimator() {
}

CardinalityValue FilterInfoWithTotalDomains::GetDistinctCount() const {
	return distinct_count;
}

ExpressionType FilterInfoWithTotalDomains::GetComparisonType() {
	return GetPredicate().GetComparisonType();
}

FilterInfo &FilterInfoWithTotalDomains::GetFilter() const {
	return GetPredicate().GetFilter();
}

JoinPredicate &FilterInfoWithTotalDomains::GetPredicate() const {
	return predicate.get();
}

void CardinalityEstimatorState::InitializeEdgeIndex() {
	edges_by_relation.resize(leaves.size());
	for (idx_t i = 0; i < edges.size(); i++) {
		auto &scope = edges[i].GetFilter().set.get();
		if (scope.Empty()) {
			unscoped_edges.push_back(i);
			continue;
		}
		auto anchor = scope.relations[0].index;
		for (idx_t j = 1; j < scope.count; j++) {
			auto relation = scope.relations[j].index;
			if (edges_by_relation[relation].size() < edges_by_relation[anchor].size()) {
				anchor = relation;
			}
		}
		edges_by_relation[anchor].push_back(i);
	}
}

bool CardinalityEstimatorState::TryGetCandidateEdges(const JoinRelationSet &set, vector<idx_t> &candidates) const {
	idx_t count = unscoped_edges.size();
	for (idx_t i = 0; i < set.count; i++) {
		count += edges_by_relation[set.relations[i].index].size();
	}
	// Collecting and sorting most edges costs more than scanning them in place.
	if (count >= edges.size() / 2) {
		return false;
	}
	candidates.reserve(count);
	candidates.insert(candidates.end(), unscoped_edges.begin(), unscoped_edges.end());
	for (idx_t i = 0; i < set.count; i++) {
		auto &entries = edges_by_relation[set.relations[i].index];
		candidates.insert(candidates.end(), entries.begin(), entries.end());
	}
	// Each edge occurs once. Restore the estimator's order before applying any selectivity.
	std::sort(candidates.begin(), candidates.end());
	return true;
}

// The filter was made on top of a logical sample or other projection,
// but no specific columns are referenced. See issue 4978 number 4.
bool CardinalityEstimator::EmptyFilter(const FilterInfo &filter_info) {
	if (!filter_info.left_set && !filter_info.right_set) {
		return true;
	}
	return false;
}

void CardinalityEstimator::AddRelationStats(const FilterInfo &filter_info) {
	D_ASSERT(!filter_info.set.get().Empty());
	// Use whichever binding is valid: prefer left_binding, fall back to right_binding.
	// left_binding may be INVALID_INDEX when left_set is empty (e.g. residual predicates
	// or single-column filters where only the right side references a relation).
	auto binding = filter_info.left_binding;
	if (!binding.table_index.IsValid()) {
		binding = filter_info.right_binding;
	}
	if (!binding.table_index.IsValid()) {
		// No valid binding (EmptyFilter), nothing to record
		return;
	}
	for (const RelationsSetToStats &r2tdom : state->relation_set_stats) {
		auto &i_set = r2tdom.equivalent_relations;
		if (i_set.find(binding) != i_set.end()) {
			// found an equivalent filter
			return;
		}
	}

	auto key = ColumnBinding(binding.table_index, binding.column_index);
	RelationsSetToStats new_r2tdom(column_binding_set_t({key}));

	state->relation_set_stats.emplace_back(new_r2tdom);
}

bool CardinalityEstimator::SingleColumnFilter(const FilterInfo &filter_info) {
	if (filter_info.left_set && filter_info.right_set && filter_info.set.get().count > 1) {
		// Both set and are from different relations
		return false;
	}
	if (EmptyFilter(filter_info)) {
		return false;
	}
	if (filter_info.join_type == JoinType::SEMI || filter_info.join_type == JoinType::ANTI) {
		return false;
	}
	return true;
}

void CardinalityEstimator::InitEquivalentRelations() {
	state->relation_set_stats.clear();
	state->residual_filters.clear();

	for (auto predicate_ref : predicate_model.GetPredicates()) {
		auto &filter = predicate_ref.get().GetFilter();
		if (state->leaf_filters.count(filter.filter_index)) {
			continue;
		}
		if (predicate_ref.get().IsResidualFilter()) {
			auto selectivity = GetResidualOnSelectivity(*filter.filter);
			auto constant = filter.filter->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
			state->residual_filters.push_back({filter, selectivity, constant && selectivity.IsZero()});
			continue;
		}
		if (SingleColumnFilter(filter)) {
			// Filter on one relation, (i.e. string or range filter on a column).
			// Grab the first relation and add it to  the equivalence_relations
			AddRelationStats(filter);
		}
	}

	for (auto &equality_class : predicate_model.GetEqualityClasses()) {
		if (equality_class.columns.empty()) {
			continue;
		}
		state->relation_set_stats.emplace_back(equality_class.columns);
		for (auto &edge : equality_class.edges) {
			state->relation_set_stats.back().predicates.push_back(edge.predicate);
		}
	}

	for (auto predicate_ref : predicate_model.GetSelectivityPredicates()) {
		auto &predicate = predicate_ref.get();
		D_ASSERT(predicate.HasValidJoinEndpoints());
		D_ASSERT(predicate.HasAnyValidStatsBinding());

		column_binding_set_t domain_group;
		if (predicate.GetLeftStatsBinding().table_index.IsValid()) {
			domain_group.insert(predicate.GetLeftStatsBinding());
		}
		if (predicate.GetRightStatsBinding().table_index.IsValid()) {
			domain_group.insert(predicate.GetRightStatsBinding());
		}
		D_ASSERT(!domain_group.empty());
		state->relation_set_stats.emplace_back(domain_group);
		state->relation_set_stats.back().predicates.push_back(predicate);
	}
}

CardinalityValue CardinalityEstimator::GetNumerator(JoinRelationSet &set) {
	CardinalityValue numerator;
	for (idx_t i = 0; i < set.count; i++) {
		auto cardinality = state->leaves.at(set.relations[i].index).cardinality;
		numerator *= cardinality;
	}
	return numerator;
}

bool CardinalityEstimator::IsFilterApplicable(const FilterInfo &filter, JoinRelationSet &set) const {
	if ((filter.must_remain_at_root || filter.set.get().Empty()) && set.count != state->leaves.size()) {
		return false;
	}
	if (!filter.set.get().Empty() && !JoinRelationSet::IsSubset(set, filter.set)) {
		return false;
	}
	if (filter.source_operator_index.IsValid()) {
		auto &op = state->operators.at(filter.source_operator_index.GetIndex()).get();
		if (JoinOrderConflictDetector::MustApplyAsOperator(op)) {
			return JoinOrderConflictDetector::IsCompletedBy(op, set);
		}
	}
	return true;
}

// Apply the selectivity multiplier for a given comparison type and effective distinct count.
static CardinalityValue ApplyComparisonRatio(CardinalityValue base_selectivity, ExpressionType comparison_type,
                                             CardinalityValue effective_d) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return base_selectivity / CardinalityValue(effective_d);
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return effective_d > 1 ? base_selectivity * CardinalityValue(effective_d - 1) / CardinalityValue(effective_d)
		                       : base_selectivity;
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
		// Model strict comparisons as half the cross product, including at small domains.
		return base_selectivity / CardinalityValue(2);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return base_selectivity * CardinalityValue(effective_d + 1) / CardinalityValue(2 * effective_d);
	default:
		return base_selectivity;
	}
}

static CardinalityValue GetEffectiveDistinctCount(CardinalityValue distinct_count) {
	return distinct_count.IsZero() ? CardinalityValue(1) : distinct_count;
}

CardinalityValue CardinalityEstimator::CalculateInnerJoinSelectivity(CardinalityValue base_selectivity,
                                                                     FilterInfoWithTotalDomains &filter) {
	auto effective_d = GetEffectiveDistinctCount(filter.GetDistinctCount());
	auto comparison_type = filter.GetComparisonType();
	if (comparison_type == ExpressionType::INVALID) {
		return base_selectivity / CardinalityValue(effective_d);
	}
	return ApplyComparisonRatio(base_selectivity, comparison_type, effective_d);
}

// MARK introduces an output column in a synthetic slot after its RHS leaf's columns.
// Only physical input columns participate in the leaf equality model.
static bool IsInputColumn(const vector<RelationStats> &leaves, ColumnBinding binding) {
	return binding.table_index.index < leaves.size() &&
	       binding.column_index.GetIndex() < leaves[binding.table_index.index].columns.size();
}

static CardinalityValue EstimateEqualityFactor(const EqualityCardinalityModel &model,
                                               const vector<RelationStats> &leaves,
                                               const vector<pair<ColumnBinding, ColumnBinding>> &equalities,
                                               const vector<idx_t> &relations) {
	auto estimate = model.Estimate(model.Components(equalities), relations);
	if (!estimate.maximum) {
		return estimate.selectivity;
	}
	CardinalityValue numerator;
	for (auto relation : relations) {
		auto rows = leaves[relation].cardinality;
		numerator *= rows;
	}
	if (numerator.IsZero()) {
		return estimate.selectivity;
	}
	auto bounded = *estimate.maximum / numerator;
	// An equal bound must not introduce cancelling row products into the factor.
	return bounded < estimate.selectivity ? bounded : estimate.selectivity;
}

CardinalityValue CardinalityEstimator::GetSelectivity(JoinRelationSet &set) {
	vector<pair<ColumnBinding, ColumnBinding>> equalities;
	CardinalityValue residual(1);
	auto process_edge = [&](FilterInfoWithTotalDomains &edge) {
		auto &predicate = edge.GetPredicate();
		auto &filter = predicate.GetFilter();
		if (predicate.GetJoinType() != JoinType::INNER || filter.left_set == filter.right_set ||
		    !IsFilterApplicable(filter, set) || edge.residual) {
			return;
		}
		if (predicate.IsEquivalencePredicate() && predicate.HasValidEqualityBindings() &&
		    IsInputColumn(state->leaves, predicate.GetLeftEqualityBinding()) &&
		    IsInputColumn(state->leaves, predicate.GetRightEqualityBinding())) {
			equalities.emplace_back(predicate.GetLeftEqualityBinding(), predicate.GetRightEqualityBinding());
		} else {
			// Unsupported expressions and non-equalities retain their explicit comparison fallback.
			residual = CalculateInnerJoinSelectivity(residual, edge);
		}
	};
	vector<idx_t> candidates;
	if (state->TryGetCandidateEdges(set, candidates)) {
		for (auto index : candidates) {
			process_edge(state->edges[index]);
		}
	} else {
		for (auto &edge : state->edges) {
			process_edge(edge);
		}
	}
	vector<idx_t> relations;
	relations.reserve(set.count);
	for (idx_t i = 0; i < set.count; i++) {
		relations.push_back(set.relations[i].index);
	}
	return EstimateEqualityFactor(*state->equalities, state->leaves, equalities, relations) * residual;
}

vector<pair<ColumnBinding, ColumnBinding>> CardinalityEstimator::GetInnerEqualities(JoinRelationSet &set,
                                                                                    InnerEqualityView view) const {
	vector<pair<ColumnBinding, ColumnBinding>> result;
	for (auto &edge : state->edges) {
		auto &predicate = edge.GetPredicate();
		auto &filter = predicate.GetFilter();
		if (predicate.IsEquivalencePredicate() && predicate.HasValidEqualityBindings() &&
		    IsInputColumn(state->leaves, predicate.GetLeftEqualityBinding()) &&
		    IsInputColumn(state->leaves, predicate.GetRightEqualityBinding()) && filter.left_set != filter.right_set &&
		    !edge.residual && IsFilterApplicable(filter, set)) {
			if (view == InnerEqualityView::DIRECT_VALUE && !predicate.IsDirectValueEquality()) {
				continue;
			}
			result.emplace_back(predicate.GetLeftEqualityBinding(), predicate.GetRightEqualityBinding());
		}
	}
	return result;
}

CardinalityValue CardinalityEstimator::GetInnerSelectivity(JoinRelationSet &set) {
	auto entry = state->inner_selectivities.find(set);
	if (entry != state->inner_selectivities.end()) {
		return entry->second;
	}
	auto result = GetSelectivity(set);
	for (auto &filter : state->residual_filters) {
		if (IsFilterApplicable(filter.filter, set)) {
			result *= filter.selectivity;
		}
	}
	state->inner_selectivities.emplace(set, result);
	return result;
}

CardinalityValue CardinalityEstimator::GetJoinSelectivity(JoinRelationSet &set, JoinRelationSet &left,
                                                          JoinRelationSet &right) {
	// Leaf cardinalities cancel because the inputs partition the combined set.
	auto combined = GetInnerSelectivity(set);
	auto left_domains = GetInnerSelectivity(left);
	auto right_domains = GetInnerSelectivity(right);
	if (left_domains.IsZero() || right_domains.IsZero()) {
		// Conditioning on a statistical zero has no defined ratio. Retain the
		// already selected input rows; this supplies no emptiness or coverage proof.
		return CardinalityValue(1);
	}
	return combined / left_domains / right_domains;
}

optional_ptr<const LogicalJoinStats> CardinalityEstimator::FindBoundary(JoinRelationSet &set,
                                                                        optional_ptr<JoinRelationSet> &region) {
	optional_ptr<const LogicalJoinStats> boundary;
	for (auto &candidate : state->boundaries) {
		if (!JoinOrderConflictDetector::IsCompletedBy(candidate.op, set)) {
			continue;
		}
		auto &projected = set_manager.Intersection(set, candidate.scope);
		// Original scopes are nested or disjoint; postorder indexes break ties after projection.
		if (!region || projected.count > region->count ||
		    (projected.count == region->count && candidate.op.get().index > boundary->op.get().index)) {
			boundary = candidate;
			region = projected;
		}
	}
	return boundary;
}

static bool ContainsBinding(const JoinRelationSet &set, ColumnBinding binding) {
	if (!binding.table_index.IsValid()) {
		return false;
	}
	for (idx_t i = 0; i < set.count; i++) {
		if (set.relations[i].index == binding.table_index.index) {
			return true;
		}
	}
	return false;
}

SemiAntiJoinCardinalityEstimate CardinalityEstimator::EstimateMembership(const LogicalJoinStats &boundary,
                                                                         JoinRelationSet &left, JoinRelationSet &right,
                                                                         vector<SemiAntiJoinDomain> &domains) {
	auto has_residual = boundary.has_residual;
	for (auto &key : boundary.equality_keys) {
		auto lhs = ProjectColumn(left, key.first);
		auto rhs = ProjectColumn(right, key.second);
		if (!lhs || !rhs) {
			has_residual = true;
			continue;
		}
		auto supported = rhs->GetSupportedSemiAntiDomainSize();
		auto total = RelationStatisticsHelper::SelectTotalDomain({lhs->total_domain, rhs->total_domain});
		if (!supported || !total || total->distinct_count.IsZero()) {
			has_residual = true;
			continue;
		}
		// Row-count fallbacks bound domains but do not estimate membership coverage.
		if (!total->has_usable_domain) {
			has_residual = true;
		}
		domains.push_back({total->distinct_count, *supported, key.first, key.second});
	}
	auto type = boundary.op.get().type == JoinOrderOperatorType::SEMI ? JoinType::SEMI : JoinType::ANTI;
	auto &left_stats = state->cardinalities.at(left);
	auto &right_stats = state->cardinalities.at(right);
	return RelationStatisticsHelper::EstimateSemiAntiJoinCardinality(
	    left_stats.cardinality, right_stats.cardinality, right_stats.row_retention, type, domains, has_residual);
}

CardinalityValue CardinalityEstimator::EstimateCardinality(JoinRelationSet &set) {
	D_ASSERT(!state->leaves.empty() && !set.Empty());
	auto entry = state->cardinalities.find(set);
	if (entry != state->cardinalities.end()) {
		return entry->second.cardinality;
	}
	optional_ptr<JoinRelationSet> region;
	auto boundary = FindBoundary(set, region);
	CardinalityValue result;
	CardinalityValue retention(1);
	optional_idx maximum;
	if (!boundary) {
		result = GetNumerator(set) * GetInnerSelectivity(set);
		maximum = optional_idx(1);
		for (idx_t i = 0; i < set.count; i++) {
			maximum = RelationStatisticsHelper::MultiplyCardinalityBounds(
			    maximum, state->leaves.at(set.relations[i].index).max_cardinality);
		}
	} else if (region->count < set.count) {
		auto &remaining = set_manager.Difference(set, *region);
		result =
		    EstimateCardinality(*region) * EstimateCardinality(remaining) * GetJoinSelectivity(set, *region, remaining);
		maximum = RelationStatisticsHelper::MultiplyCardinalityBounds(state->cardinalities.at(*region).maximum,
		                                                              state->cardinalities.at(remaining).maximum);
	} else {
		auto &op = boundary->op.get();
		auto &left = set_manager.Intersection(set, op.left_relations);
		auto &right = set_manager.Intersection(set, op.right_relations);
		D_ASSERT(!left.Empty() && !right.Empty() && left.count < set.count && right.count < set.count);
		auto left_rows = EstimateCardinality(left);
		auto right_rows = EstimateCardinality(right);
		if (op.type == JoinOrderOperatorType::LEFT) {
			result = MaxValue(left_rows, left_rows * right_rows * GetLeftJoinSelectivity(op, left, right));
			maximum = RelationStatisticsHelper::JoinCardinalityBound(
			    JoinType::LEFT, state->cardinalities.at(left).maximum, state->cardinalities.at(right).maximum);
		} else if (op.type == JoinOrderOperatorType::MARK) {
			result = left_rows;
			maximum = state->cardinalities.at(left).maximum;
			retention = state->cardinalities.at(left).row_retention;
		} else {
			auto type = op.type == JoinOrderOperatorType::SEMI ? JoinType::SEMI : JoinType::ANTI;
			vector<SemiAntiJoinDomain> domains;
			result = EstimateMembership(*boundary, left, right, domains).cardinality;
			maximum = RelationStatisticsHelper::JoinCardinalityBound(type, state->cardinalities.at(left).maximum,
			                                                         state->cardinalities.at(right).maximum);
			retention = left_rows.IsZero() ? CardinalityValue(0)
			                               : state->cardinalities.at(left).row_retention *
			                                     MinValue(CardinalityValue(1), result / left_rows);
		}
		// Filters above the boundary see its output, including NULL-extended LEFT rows.
		auto post_selectivity = GetJoinSelectivity(set, left, right);
		result *= post_selectivity;
		retention *= MinValue(CardinalityValue(1), post_selectivity);
	}
	for (auto &filter : state->residual_filters) {
		if (filter.proven_empty && IsFilterApplicable(filter.filter, set)) {
			maximum = optional_idx(0);
		}
	}
	RelationStats output;
	output.cardinality = result;
	output.max_cardinality = maximum;
	result = output.GetCardinality();
	state->cardinalities.emplace(set, RegionEstimate {result, maximum, retention});
	return result;
}

CardinalityValue CardinalityEstimator::EstimateCardinalityForCost(JoinRelationSet &set) {
	RelationStats rows;
	rows.cardinality = EstimateCardinality(set);
	rows.max_cardinality = state->cardinalities.at(set).maximum;
	return rows.GetCardinalityForCost();
}

optional<RelationColumnStats> CardinalityEstimator::ProjectInputColumn(JoinRelationSet &set, ColumnBinding binding,
                                                                       optional_ptr<const LogicalJoinStats> boundary,
                                                                       optional_ptr<JoinRelationSet> region) {
	if (!boundary) {
		if (binding.table_index.index >= state->leaves.size()) {
			return {};
		}
		auto &columns = state->leaves[binding.table_index.index].columns;
		if (binding.column_index.GetIndex() >= columns.size()) {
			return {};
		}
		auto result = columns[binding.column_index.GetIndex()];
		if (set.count > 1) {
			result.semi_anti_join_domain_evidence.Invalidate();
		}
		return result;
	}
	if (region->count < set.count) {
		auto &input = ContainsBinding(*region, binding) ? *region : set_manager.Difference(set, *region);
		auto result = ProjectColumn(input, binding);
		if (result) {
			result->semi_anti_join_domain_evidence.Invalidate();
		}
		return result;
	}
	auto &op = boundary->op.get();
	auto &left = set_manager.Intersection(set, op.left_relations);
	auto &right = set_manager.Intersection(set, op.right_relations);
	bool on_left = ContainsBinding(left, binding);
	if (!on_left && op.type == JoinOrderOperatorType::MARK) {
		auto rhs = op.right_relations.get().relations[0];
		if (binding.table_index.index == rhs.index &&
		    binding.column_index.GetIndex() == state->leaves[rhs.index].columns.size()) {
			return RelationColumnStats(binding, DistinctCount(3, DistinctCountSource::CARDINALITY), Identifier("mark"));
		}
		return {};
	}
	if (!on_left && op.type != JoinOrderOperatorType::LEFT) {
		return {};
	}
	auto result = ProjectColumn(on_left ? left : right, binding);
	if (!result) {
		return {};
	}
	auto left_rows = state->cardinalities.at(left).cardinality;
	CardinalityValue join_rows;
	if (op.type == JoinOrderOperatorType::LEFT) {
		join_rows = MaxValue(left_rows, left_rows * state->cardinalities.at(right).cardinality *
		                                    GetLeftJoinSelectivity(op, left, right));
		result->semi_anti_join_domain_evidence.Invalidate();
		if (!on_left) {
			result->can_have_null = true;
		}
	} else if (op.type == JoinOrderOperatorType::MARK) {
		join_rows = left_rows;
	} else {
		vector<SemiAntiJoinDomain> domains;
		auto estimate = EstimateMembership(*boundary, left, right, domains);
		join_rows = estimate.cardinality;
		result->current_domain =
		    RelationStatisticsHelper::EstimateCurrentDomain(result->current_domain, left_rows, join_rows);
		if (op.type == JoinOrderOperatorType::SEMI) {
			for (auto &domain : domains) {
				if (domain.preserved_binding == binding) {
					result->current_domain.distinct_count =
					    MinValue(result->current_domain.distinct_count, domain.rhs_current_domain);
				}
			}
		}
	}
	auto post_selectivity = GetJoinSelectivity(set, left, right);
	result->current_domain = RelationStatisticsHelper::EstimateCurrentDomain(result->current_domain, join_rows,
	                                                                         join_rows * post_selectivity);
	return result;
}

optional<RelationColumnStats>
CardinalityEstimator::ProjectColumn(JoinRelationSet &set, ColumnBinding binding,
                                    optional_ptr<const vector<EqualityComponent>> components) {
	if (!ContainsBinding(set, binding)) {
		return {};
	}
	auto rows = EstimateCardinality(set);
	auto cached = state->projected_columns.find(set);
	if (cached != state->projected_columns.end()) {
		auto column = cached->second.find(binding);
		if (column != cached->second.end()) {
			return column->second;
		}
	}
	optional_ptr<JoinRelationSet> region;
	auto boundary = FindBoundary(set, region);
	auto result = ProjectInputColumn(set, binding, boundary, region);
	if (!result) {
		return {};
	}
	if (!IsInputColumn(state->leaves, binding)) {
		result->current_domain.distinct_count = MinValue(result->current_domain.distinct_count, rows);
		state->projected_columns[set].emplace(binding, *result);
		return result;
	}
	auto normalized = state->equalities->Normalize(binding);
	vector<EqualityComponent> local_components;
	if (!components) {
		local_components = state->equalities->Components(GetInnerEqualities(set, InnerEqualityView::STATISTICAL));
		components = &local_components;
	}
	for (auto &component : *components) {
		if (std::find(component.bindings.begin(), component.bindings.end(), normalized) == component.bindings.end()) {
			continue;
		}
		vector<EqualitySupport> supports;
		for (auto &other : component.bindings) {
			auto input = ProjectInputColumn(set, other, boundary, region);
			if (!input) {
				break;
			}
			supports.push_back({input->total_domain.distinct_count, input->current_domain.distinct_count,
			                    input->support_identities, input->total_domain.has_usable_domain});
		}
		if (supports.size() == component.bindings.size()) {
			CardinalityValue factor;
			auto overlap = state->equalities->Intersect(supports, factor);
			result->total_domain =
			    DistinctCount(overlap.total, DistinctCountSource::DERIVED, overlap.has_usable_domain);
			result->current_domain =
			    DistinctCount(overlap.current, DistinctCountSource::CARDINALITY, overlap.has_usable_domain);
			result->support_identities.reset();
		}
	}
	result->current_domain.distinct_count =
	    MinValue(MinValue(result->current_domain.distinct_count, result->total_domain.distinct_count), rows);
	if (result->support_identities && !PreservesSupport(set, binding.table_index.index)) {
		result->support_identities.reset();
	}
	if (boundary) {
		state->projected_columns[set].emplace(binding, *result);
	}
	return result;
}

bool CardinalityEstimator::PreservesSupport(JoinRelationSet &set, idx_t relation) {
	if (set.count == 1) {
		return set.relations[0].index == relation;
	}
	optional_ptr<JoinRelationSet> region;
	auto boundary = FindBoundary(set, region);
	if (!boundary || region != &set ||
	    (boundary->op.get().type != JoinOrderOperatorType::LEFT &&
	     boundary->op.get().type != JoinOrderOperatorType::MARK)) {
		return false;
	}
	auto &op = boundary->op.get();
	auto &left = set_manager.Intersection(set, op.left_relations);
	auto &right = set_manager.Intersection(set, op.right_relations);
	if (!ContainsBinding(left, ColumnBinding(TableIndex(relation), ProjectionIndex(0)))) {
		return false;
	}
	for (auto &predicate_ref : predicate_model.GetPredicates()) {
		auto &predicate = predicate_ref.get();
		auto &filter = predicate.GetFilter();
		if (predicate.GetJoinType() == JoinType::INNER && IsFilterApplicable(filter, set) &&
		    !IsFilterApplicable(filter, left) && !IsFilterApplicable(filter, right)) {
			return false;
		}
	}
	// LEFT and MARK preserve the complete support of their left input.
	return PreservesSupport(left, relation);
}

optional<RelationStats> CardinalityEstimator::ProjectStats(JoinRelationSet &set,
                                                           const vector<ColumnBinding> &bindings) {
	if (set.count == 1) {
		return RelationStatisticsHelper::ProjectOutputStats(state->leaves[set.relations[0].index], bindings);
	}
	RelationStats result;
	result.cardinality = EstimateCardinality(set);
	auto &estimate = state->cardinalities.at(set);
	result.max_cardinality = estimate.maximum;
	result.row_retention = estimate.row_retention;
	auto equalities = GetInnerEqualities(set, InnerEqualityView::STATISTICAL);
	auto components = state->equalities->Components(equalities);
	result.columns.reserve(bindings.size());
	for (auto &binding : bindings) {
		auto column = ProjectColumn(set, binding, &components);
		if (!column) {
			return {};
		}
		result.columns.push_back(*column);
	}
	auto value_equalities = GetInnerEqualities(set, InnerEqualityView::DIRECT_VALUE);
	vector<bool> hidden(state->leaves.size(), false);
	vector<EqualityProjection> projections(state->leaves.size());
	for (auto &boundary : state->boundaries) {
		if (!JoinOrderConflictDetector::IsCompletedBy(boundary.op, set)) {
			continue;
		}
		auto &op = boundary.op.get();
		if (op.type == JoinOrderOperatorType::SEMI) {
			for (auto &key : boundary.value_equalities) {
				if (IsInputColumn(state->leaves, key.first) && IsInputColumn(state->leaves, key.second)) {
					value_equalities.push_back(key);
				}
			}
		}
		for (idx_t i = 0; i < op.right_relations.get().count; i++) {
			auto r = op.right_relations.get().relations[i].index;
			if (op.type == JoinOrderOperatorType::LEFT) {
				projections[r].null_extended = true;
			} else {
				hidden[r] = true;
			}
		}
		if (op.type == JoinOrderOperatorType::LEFT) {
			for (auto &key : op.costing_equalities) {
				if (ContainsBinding(set, key.first) && ContainsBinding(set, key.second) &&
				    IsInputColumn(state->leaves, key.first) && IsInputColumn(state->leaves, key.second)) {
					equalities.push_back(key);
				}
			}
			for (auto &key : boundary.equality_keys) {
				if (ContainsBinding(set, key.first) && ContainsBinding(set, key.second) &&
				    IsInputColumn(state->leaves, key.first) && IsInputColumn(state->leaves, key.second)) {
					equalities.push_back(key);
				}
			}
		}
	}
	vector<idx_t> participants;
	for (idx_t i = 0; i < set.count; i++) {
		auto r = set.relations[i].index;
		if (!hidden[r]) {
			participants.push_back(r);
		}
	}
	auto multiplicity_components = state->equalities->Components(equalities);
	for (auto r : participants) {
		auto &projection = projections[r];
		projection.preserve_keys = !projection.null_extended && !state->leaves[r].unique_keys.empty() &&
		                           state->equalities->PreservesMultiplicity(r, participants, multiplicity_components);
		projection.preserve_identity =
		    (!state->leaves[r].joint_stats.empty() || state->leaves[r].tuple_identities) && PreservesSupport(set, r);
		projection.selection = projection.preserve_identity ? EqualitySelection::ALL : EqualitySelection::MATCHED;
	}
	// The relational operator owns introduced columns; the leaf model projects only input evidence.
	vector<RelationColumnStats> introduced;
	if (std::any_of(result.columns.begin(), result.columns.end(),
	                [&](const RelationColumnStats &column) { return !IsInputColumn(state->leaves, column.binding); })) {
		vector<RelationColumnStats> input_columns;
		for (auto &column : result.columns) {
			if (IsInputColumn(state->leaves, column.binding)) {
				input_columns.push_back(std::move(column));
			} else {
				introduced.push_back(std::move(column));
			}
		}
		result.columns = std::move(input_columns);
	}
	state->equalities->ProjectEvidence(result, components, projections, value_equalities);
	result.stats_initialized = true;
	if (!introduced.empty()) {
		for (auto &column : introduced) {
			result.columns.push_back(std::move(column));
		}
		return RelationStatisticsHelper::ProjectOutputStats(result, bindings);
	}
	result.Verify(bindings);
	return result;
}

static CardinalityValue GetResidualOnSelectivity(const Expression &expression) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		auto &value = expression.Cast<BoundConstantExpression>().GetValue();
		return CardinalityValue(!value.IsNull() && value.GetValue<bool>() ? 1 : 0);
	}
	return CardinalityValue(RelationStatisticsHelper::DEFAULT_SELECTIVITY);
}

CardinalityValue CardinalityEstimator::GetLeftJoinSelectivity(const JoinOrderOperator &join_operator,
                                                              JoinRelationSet &left, JoinRelationSet &right) {
	CardinalityValue selectivity;
	vector<pair<ColumnBinding, ColumnBinding>> equalities;
	auto in_inputs = [&](ColumnBinding binding) {
		return (ContainsBinding(left, binding) || ContainsBinding(right, binding)) &&
		       IsInputColumn(state->leaves, binding);
	};
	for (auto &equality : join_operator.costing_equalities) {
		if (in_inputs(equality.first) && in_inputs(equality.second)) {
			equalities.push_back(equality);
		}
	}
	unordered_set<idx_t> accounted_predicates;
	for (auto &edge : state->edges) {
		auto &predicate = edge.GetPredicate();
		auto source = predicate.GetFilter().source_operator_index;
		if (!source.IsValid() || source.GetIndex() != join_operator.index) {
			continue;
		}
		accounted_predicates.insert(predicate.GetFilter().filter_index);
		auto comparison = predicate.GetComparisonType();
		if ((comparison == ExpressionType::COMPARE_EQUAL || comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) &&
		    predicate.HasValidEqualityBindings() && in_inputs(predicate.GetLeftEqualityBinding()) &&
		    in_inputs(predicate.GetRightEqualityBinding())) {
			equalities.emplace_back(predicate.GetLeftEqualityBinding(), predicate.GetRightEqualityBinding());
		} else {
			selectivity = CalculateInnerJoinSelectivity(selectivity, edge);
		}
	}
	vector<idx_t> relations;
	relations.reserve(left.count + right.count);
	CardinalityValue input_factor;
	for (auto input : {&left, &right}) {
		auto input_equalities = GetInnerEqualities(*input, InnerEqualityView::STATISTICAL);
		vector<idx_t> input_relations;
		input_relations.reserve(input->count);
		for (idx_t i = 0; i < input->count; i++) {
			input_relations.push_back(input->relations[i].index);
		}
		input_factor *= EstimateEqualityFactor(*state->equalities, state->leaves, input_equalities, input_relations);
		relations.insert(relations.end(), input_relations.begin(), input_relations.end());
		equalities.insert(equalities.end(), input_equalities.begin(), input_equalities.end());
	}
	// Input rows already include their own equalities. Only the incremental ON factor applies at this boundary.
	selectivity *=
	    input_factor.IsZero()
	        ? CardinalityValue(0)
	        : EstimateEqualityFactor(*state->equalities, state->leaves, equalities, relations) / input_factor;
	for (auto index : join_operator.costing_predicate_indices) {
		if (!accounted_predicates.count(index)) {
			selectivity *= CardinalityValue(RelationStatisticsHelper::DEFAULT_SELECTIVITY);
		}
	}
	for (auto &residual : join_operator.residual_predicates) {
		selectivity *= GetResidualOnSelectivity(residual);
	}
	return selectivity;
}

void CardinalityEstimator::Initialize(vector<RelationStats> relations,
                                      const vector<unique_ptr<JoinOrderOperator>> &operators,
                                      const join_column_sources_t &sources) {
	D_ASSERT(state->leaves.empty());
	state->leaves = std::move(relations);
	// Region-local single-input filters commute to that input. Capture their effect once,
	// while expressions and column bindings still describe the original input.
	map<idx_t, vector<unique_ptr<Expression>>> leaf_expressions;
	for (auto predicate : predicate_model.GetPredicates()) {
		auto &filter = predicate.get().GetFilter();
		if (filter.from_logical_filter && filter.set.get().count == 1 && !filter.must_remain_at_root) {
			leaf_expressions[filter.set.get().relations[0].index].push_back(filter.filter->Copy());
			state->leaf_filters.insert(filter.filter_index);
		}
	}
	for (auto &entry : leaf_expressions) {
		auto &leaf = state->leaves[entry.first];
		vector<ColumnBinding> bindings;
		for (auto &column : leaf.columns) {
			bindings.push_back(column.binding);
		}
		leaf = RelationStatisticsHelper::ConditionFilterStats(leaf, entry.second, bindings);
	}
	InitEquivalentRelations();
	for (idx_t i = 0; i < state->leaves.size(); i++) {
		auto &stats = state->leaves[i];
		D_ASSERT(stats.stats_initialized);
		auto cardinality = stats.GetCardinality();
		stats.cardinality = cardinality;
		for (idx_t column = 0; column < stats.columns.size(); column++) {
			stats.columns[column].binding = ColumnBinding(TableIndex(i), ProjectionIndex(column));
		}
	}
	state->equalities = make_uniq<EqualityCardinalityModel>(state->leaves, sources);
	for (idx_t i = 0; i < state->leaves.size(); i++) {
		UpdateTotalDomains(RelationIndex(i), state->leaves[i], sources);
	}
	for (auto &op : operators) {
		if (op->type != JoinOrderOperatorType::MARK) {
			continue;
		}
		D_ASSERT(op->right_relations.get().count == 1);
		auto rhs = op->right_relations.get().relations[0];
		// The marker's synthetic slot follows the RHS columns but is produced by the join.
		auto key = ColumnBinding(TableIndex(rhs.index), ProjectionIndex(state->leaves[rhs.index].columns.size()));
		for (auto &domain : state->relation_set_stats) {
			if (domain.equivalent_relations.count(key)) {
				domain.domain_estimate.Update(key, DistinctCount(3, DistinctCountSource::CARDINALITY), nullptr);
			}
		}
	}
	for (auto &domain : state->relation_set_stats) {
		for (auto &predicate : domain.predicates) {
			state->edges.emplace_back(predicate, domain);
		}
	}
	state->InitializeEdgeIndex();
	for (idx_t i = 0; i < state->leaves.size(); i++) {
		auto &stats = state->leaves[i];
		state->cardinalities.emplace(set_manager.GetJoinRelation(RelationIndex(i)),
		                             RegionEstimate {stats.cardinality, stats.max_cardinality, stats.row_retention});
	}
	state->operators.reserve(operators.size());
	for (auto &op : operators) {
		D_ASSERT(op->index == state->operators.size());
		state->operators.push_back(*op);
		if (op->type == JoinOrderOperatorType::INNER || op->type == JoinOrderOperatorType::CROSS_PRODUCT) {
			continue;
		}
		state->boundaries.emplace_back(*op, set_manager.Union(op->left_relations, op->right_relations));
		auto &boundary = state->boundaries.back();
		for (auto &condition : op->conditions) {
			boundary.has_residual |= !condition.IsComparison() ||
			                         (condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL &&
			                          condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		}
		idx_t predicate_count = 0;
		for (auto &predicate_ref : predicate_model.GetPredicates()) {
			auto &predicate = predicate_ref.get();
			auto source = predicate.GetFilter().source_operator_index;
			if (!source.IsValid() || source.GetIndex() != op->index) {
				continue;
			}
			predicate_count++;
			auto type = predicate.GetComparisonType();
			if ((type != ExpressionType::COMPARE_EQUAL && type != ExpressionType::COMPARE_NOT_DISTINCT_FROM) ||
			    !predicate.HasValidEqualityBindings()) {
				boundary.has_residual = true;
				continue;
			}
			auto left = predicate.GetLeftEqualityBinding();
			auto right = predicate.GetRightEqualityBinding();
			if (ContainsBinding(op->right_relations, left) && ContainsBinding(op->left_relations, right)) {
				std::swap(left, right);
			}
			if (!ContainsBinding(op->left_relations, left) || !ContainsBinding(op->right_relations, right)) {
				boundary.has_residual = true;
				continue;
			}
			auto key = make_pair(left, right);
			if (op->type == JoinOrderOperatorType::SEMI && predicate.IsDirectValueEquality()) {
				boundary.value_equalities.push_back(key);
			}
			if (std::find(boundary.equality_keys.begin(), boundary.equality_keys.end(), key) ==
			    boundary.equality_keys.end()) {
				boundary.equality_keys.push_back(key);
			}
		}
		boundary.has_residual |= predicate_count < op->conditions.size();
	}
}

void CardinalityEstimator::UpdateTotalDomains(RelationIndex relation_id, const RelationStats &stats,
                                              const join_column_sources_t &sources) {
	//! Initialize the distinct count for all columns used in joins with the current relation.
	for (idx_t i = 0; i < stats.columns.size(); i++) {
		//! for every column used in a filter in the relation, get the distinct count via HLL, or assume it to be
		//! the cardinality
		// Update the relation_to_tdom set with the estimated distinct count (or tdom) calculated above
		auto key = ColumnBinding(TableIndex(relation_id.index), ProjectionIndex(i));
		auto entry = sources.find(key);
		for (auto &relation_to_tdom : state->relation_set_stats) {
			const auto &i_set = relation_to_tdom.equivalent_relations;
			if (i_set.find(key) == i_set.end()) {
				continue;
			}
			relation_to_tdom.domain_estimate.Update(key, stats.columns[i].total_domain,
			                                        entry == sources.end() ? nullptr : &entry->second);
		}
	}
}

} // namespace duckdb
