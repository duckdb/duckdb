#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/optimizer/join_order/join_order_operator.hpp"
#include "duckdb/optimizer/join_order/join_predicate.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

#include <algorithm>
#include <map>

namespace duckdb {

struct DomainEstimate {
public:
	DomainEstimate();

public:
	CardinalityValue GetDistinctCount(const JoinPredicate &predicate) const;
	bool HasReliableDistinctCount() const;
	CardinalityValue GetReliableDistinctCount() const;
	void Update(ColumnBinding binding, const DistinctCount &distinct_count,
	            optional_ptr<const JoinColumnSources> sources);
	void AddSource(const JoinColumnSource &source);
	void Restrict(idx_t bound);

private:
	optional<DistinctCount> estimate;
	//! Row-count fallbacks apply only to predicates involving their relation.
	column_binding_map_t<DistinctCount> local_fallbacks;
	column_binding_set_t sources;
	idx_t reliable_source_count = 0;
	idx_t reliable_source_cardinality = NumericLimits<idx_t>::Maximum();
	vector<CardinalityValue> min_max_candidates;
};

DomainEstimate::DomainEstimate() {
}

CardinalityValue DomainEstimate::GetDistinctCount(const JoinPredicate &predicate) const {
	auto domain = estimate;
	for (bool left : {true, false}) {
		auto entry = local_fallbacks.find(predicate.GetStatsBinding(left));
		if (entry != local_fallbacks.end()) {
			domain = domain ? RelationStatisticsHelper::SelectTotalDomain({*domain, entry->second}) : entry->second;
		}
	}
	if (!domain) {
		return CardinalityValue(static_cast<double>(NumericLimits<idx_t>::Maximum()));
	}
	auto result = domain->distinct_count;
	auto comparison = predicate.GetComparisonType();
	if (domain->source == DistinctCountSource::CARDINALITY && estimate && result > 0 &&
	    predicate.GetJoinType() == JoinType::INNER &&
	    (comparison == ExpressionType::COMPARE_EQUAL || comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM)) {
		// Discount scan-backed domains before applying predicate-local bounds.
		return MinValue(result, MaxValue(CardinalityValue(1),
		                                 estimate->distinct_count * RelationStatisticsHelper::DEFAULT_SELECTIVITY));
	}
	if (!HasReliableDistinctCount() || reliable_source_count != 1) {
		return result;
	}
	optional<CardinalityValue> key_domain;
	for (auto candidate : min_max_candidates) {
		if (candidate > result && candidate <= reliable_source_cardinality) {
			key_domain = key_domain ? MinValue(*key_domain, candidate) : candidate;
		}
	}
	return key_domain ? *key_domain : result;
}

bool DomainEstimate::HasReliableDistinctCount() const {
	return estimate && (estimate->source == DistinctCountSource::HLL || estimate->source == DistinctCountSource::EXACT);
}

CardinalityValue DomainEstimate::GetReliableDistinctCount() const {
	if (!estimate || !HasReliableDistinctCount()) {
		throw InternalException("Attempted to access an unreliable distinct count");
	}
	return estimate->distinct_count;
}

void DomainEstimate::Restrict(idx_t bound) {
	D_ASSERT(estimate);
	if (!estimate) {
		return;
	}
	estimate->distinct_count = MinValue(estimate->distinct_count, CardinalityValue(bound));
	// the filter bound holds for every column of the class; no candidate overrides it
	min_max_candidates.clear();
}

void DomainEstimate::AddSource(const JoinColumnSource &source) {
	if (!sources.insert(source.binding).second) {
		return;
	}
	if (source.domain.source == DistinctCountSource::HLL || source.domain.source == DistinctCountSource::EXACT) {
		reliable_source_cardinality = MinValue(reliable_source_cardinality, source.source_cardinality);
		reliable_source_count++;
	}
}

void DomainEstimate::Update(ColumnBinding binding, const DistinctCount &distinct_count,
                            optional_ptr<const JoinColumnSources> column_sources) {
	if (column_sources) {
		AddSource(column_sources->direct);
		for (auto &source : column_sources->equivalents) {
			AddSource(source);
		}
	} else if (distinct_count.source == DistinctCountSource::CARDINALITY) {
		local_fallbacks.emplace(binding, distinct_count);
		return;
	} else if (distinct_count.source == DistinctCountSource::HLL ||
	           distinct_count.source == DistinctCountSource::EXACT) {
		// A reliable derived domain has no known source population for the uplift guard.
		reliable_source_cardinality = 0;
	}
	if (column_sources && distinct_count.source == DistinctCountSource::MIN_MAX) {
		min_max_candidates.push_back(MinValue(distinct_count.distinct_count, column_sources->direct.scan_cardinality));
	}
	estimate = estimate ? RelationStatisticsHelper::SelectTotalDomain({*estimate, distinct_count}) : distinct_count;
}

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
	FilterInfoWithTotalDomains(JoinPredicate &predicate, RelationsSetToStats &relation_set_to_stats)
	    : predicate(predicate), domain_estimate(relation_set_to_stats.domain_estimate),
	      distinct_count(domain_estimate.get().GetDistinctCount(predicate)), residual(predicate.IsResidualFilter()) {
	}

public:
	CardinalityValue GetDistinctCount() const;
	bool HasReliableDistinctCount() const;
	CardinalityValue GetReliableDistinctCount() const;
	ExpressionType GetComparisonType();
	bool IsInnerEquality();
	FilterInfo &GetFilter() const;
	JoinPredicate &GetPredicate() const;

public:
	reference<JoinPredicate> predicate;
	reference<const DomainEstimate> domain_estimate;
	CardinalityValue distinct_count;
	bool residual;
};

struct SelectivitySubgraph {
public:
	SelectivitySubgraph() : relations(nullptr), selectivity(1) {
	}

public:
	optional_ptr<JoinRelationSet> relations;
	CardinalityValue selectivity;
};

struct CompositeJoinPairStats {
public:
	//! The row-count cap is only plausible when the candidate key cardinality is within the same order of magnitude
	//! as an observed single-column domain. Otherwise broad fact-to-fact joins can look like key lookups.
	static constexpr double MAX_CARDINALITY_TO_DISTINCT_RATIO = 8;

	void RegisterDistinctCount(CardinalityValue distinct_count) {
		if (!has_distinct_count) {
			first_distinct_count = distinct_count;
			has_distinct_count = true;
		}
		if (distinct_count > max_distinct_count) {
			max_distinct_count = distinct_count;
		}
	}

	bool CanApplyCap(CardinalityValue cap) const {
		return has_distinct_count && cap > 0 && max_distinct_count > 0 &&
		       cap <= max_distinct_count * MAX_CARDINALITY_TO_DISTINCT_RATIO;
	}

public:
	CardinalityValue first_distinct_count = 0;
	CardinalityValue max_distinct_count = 0;
	bool has_distinct_count = false;
};

struct LogicalJoinStats {
	LogicalJoinStats(const JoinOrderOperator &op, JoinRelationSet &scope, CardinalityValue selectivity)
	    : op(op), scope(scope), selectivity(selectivity) {
	}

	reference<const JoinOrderOperator> op;
	reference<JoinRelationSet> scope;
	CardinalityValue selectivity;
	vector<pair<ColumnBinding, ColumnBinding>> equality_keys;
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
	void InitializeEdgeIndex();
	bool TryGetCandidateEdges(const JoinRelationSet &set, vector<idx_t> &candidates) const;

	vector<RelationsSetToStats> relation_set_stats;
	vector<FilterInfoWithTotalDomains> edges;
	//! Each edge is indexed by one of the relations required by its filter.
	vector<vector<idx_t>> edges_by_relation;
	vector<idx_t> unscoped_edges;
	vector<RelationStats> leaves;
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

bool FilterInfoWithTotalDomains::HasReliableDistinctCount() const {
	return domain_estimate.get().HasReliableDistinctCount();
}

CardinalityValue FilterInfoWithTotalDomains::GetReliableDistinctCount() const {
	return domain_estimate.get().GetReliableDistinctCount();
}

ExpressionType FilterInfoWithTotalDomains::GetComparisonType() {
	return GetPredicate().GetComparisonType();
}

bool FilterInfoWithTotalDomains::IsInnerEquality() {
	return GetPredicate().IsEquivalencePredicate();
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
		if (predicate.GetStatsBinding(true).table_index.IsValid()) {
			domain_group.insert(predicate.GetStatsBinding(true));
		}
		if (predicate.GetStatsBinding(false).table_index.IsValid()) {
			domain_group.insert(predicate.GetStatsBinding(false));
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

static optional_ptr<JoinRelationSet> GetEdgeEndpoint(FilterInfoWithTotalDomains &edge,
                                                     JoinRelationSetManager &set_manager, bool left) {
	if (edge.GetComparisonType() != ExpressionType::INVALID) {
		auto &binding = edge.GetPredicate().GetStatsBinding(left);
		if (binding.table_index.IsValid()) {
			return &set_manager.GetJoinRelation(RelationIndex(binding.table_index.index));
		}
	}
	return left ? edge.GetPredicate().GetLeftSetOptional() : edge.GetPredicate().GetRightSetOptional();
}

bool EdgeConnects(FilterInfoWithTotalDomains &edge, SelectivitySubgraph &subgraph,
                  JoinRelationSetManager &set_manager) {
	auto left_endpoint = GetEdgeEndpoint(edge, set_manager, true);
	if (left_endpoint && JoinRelationSet::IsSubset(*subgraph.relations, *left_endpoint)) {
		return true;
	}
	auto right_endpoint = GetEdgeEndpoint(edge, set_manager, false);
	if (right_endpoint && JoinRelationSet::IsSubset(*subgraph.relations, *right_endpoint)) {
		return true;
	}
	return false;
}

vector<idx_t> SubgraphsConnectedByEdge(FilterInfoWithTotalDomains &edge, vector<SelectivitySubgraph> &subgraphs,
                                       JoinRelationSetManager &set_manager) {
	vector<idx_t> res;
	// Keep the first two matches in order: merging removes the second subgraph.
	for (idx_t index = 0; index < subgraphs.size(); index++) {
		if (EdgeConnects(edge, subgraphs.at(index), set_manager)) {
			res.push_back(index);
			if (res.size() == 2) {
				break;
			}
		}
	}
	return res;
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

static bool GetEqualityEdgeRelations(FilterInfoWithTotalDomains &edge, RelationIndex &left, RelationIndex &right) {
	if (!edge.IsInnerEquality()) {
		return false;
	}
	auto &left_binding = edge.GetPredicate().GetEqualityBinding(true);
	auto &right_binding = edge.GetPredicate().GetEqualityBinding(false);
	if (!left_binding.table_index.IsValid() || !right_binding.table_index.IsValid()) {
		return false;
	}
	left = RelationIndex(left_binding.table_index.index);
	right = RelationIndex(right_binding.table_index.index);
	return left != right;
}

static optional_ptr<JoinRelationSet> GetEqualityJoinPair(FilterInfoWithTotalDomains &edge,
                                                         JoinRelationSetManager &set_manager) {
	RelationIndex left;
	RelationIndex right;
	if (!GetEqualityEdgeRelations(edge, left, right)) {
		return nullptr;
	}
	auto &left_set = set_manager.GetJoinRelation(left);
	auto &right_set = set_manager.GetJoinRelation(right);
	return set_manager.Union(left_set, right_set);
}

CardinalityValue CardinalityEstimator::GetJoinPairCap(JoinRelationSet &join_pair) {
	D_ASSERT(join_pair.count == 2);
	auto cap = GetNumerator(set_manager.GetJoinRelation(join_pair.relations[0]));
	for (idx_t relation_index = 0; relation_index < join_pair.count; relation_index++) {
		auto &single_relation = set_manager.GetJoinRelation(join_pair.relations[relation_index]);
		auto cardinality = GetNumerator(single_relation);
		if (cardinality <= 0) {
			return 0;
		}
		cap = MinValue(cap, cardinality);
	}
	return cap;
}

CardinalityValue CardinalityEstimator::GetUniqueJoinPairDomain(JoinRelationSet &join_pair) {
	D_ASSERT(join_pair.count == 2);
	auto equality_pair = predicate_model.GetDirectEqualityPair(join_pair);
	if (!equality_pair) {
		return 0;
	}
	CardinalityValue domain(0);
	for (idx_t relation_index = 0; relation_index < join_pair.count; relation_index++) {
		auto relation = join_pair.relations[relation_index];
		auto &equality_bindings =
		    relation_index == 0 ? equality_pair->first_relation_bindings : equality_pair->second_relation_bindings;
		for (auto &unique_key : state->leaves.at(relation.index).unique_keys) {
			bool covered = !unique_key.empty();
			for (auto column : unique_key) {
				ColumnBinding binding(TableIndex(relation.index), ProjectionIndex(column));
				covered = covered && equality_bindings.find(binding) != equality_bindings.end();
			}
			if (covered) {
				// Every row has a different key combination, so the other side matches at most one row.
				domain = MaxValue(domain, GetNumerator(set_manager.GetJoinRelation(relation)));
				break;
			}
		}
	}
	return domain;
}

class CardinalityEstimator::CompositeJoinPairCaps {
public:
	explicit CompositeJoinPairCaps(CardinalityEstimator &estimator) : estimator(estimator) {
	}

	void Register(FilterInfoWithTotalDomains &edge);
	bool ApplyIncrement(CardinalityValue &target_selectivity, FilterInfoWithTotalDomains &edge, JoinRelationSet &scope);
	bool ApplyPending(CardinalityValue &target_selectivity, JoinRelationSet &scope);

private:
	bool ApplyPair(CardinalityValue &target_selectivity, JoinRelationSet &join_pair);

private:
	CardinalityEstimator &estimator;
	//! Successfully capped pairs cannot be registered again.
	reference_map_t<JoinRelationSet, CompositeJoinPairStats> pending_join_pairs;
	reference_set_t<JoinRelationSet> capped_join_pairs;
};

void CardinalityEstimator::CompositeJoinPairCaps::Register(FilterInfoWithTotalDomains &edge) {
	if (!edge.IsInnerEquality()) {
		return;
	}
	auto join_pair = GetEqualityJoinPair(edge, estimator.set_manager);
	if (!join_pair || !estimator.predicate_model.HasDirectCompositeEquality(*join_pair)) {
		return;
	}
	if (capped_join_pairs.find(*join_pair) != capped_join_pairs.end()) {
		return;
	}
	pending_join_pairs[*join_pair].RegisterDistinctCount(edge.GetDistinctCount());
}

bool CardinalityEstimator::CompositeJoinPairCaps::ApplyPair(CardinalityValue &target_selectivity,
                                                            JoinRelationSet &join_pair) {
	if (capped_join_pairs.find(join_pair) != capped_join_pairs.end()) {
		return false;
	}
	const auto it = pending_join_pairs.find(join_pair);
	if (it == pending_join_pairs.end()) {
		return false;
	}
	auto &stats = it->second;
	auto cap = estimator.GetJoinPairCap(join_pair);
	auto unique_domain = estimator.GetUniqueJoinPairDomain(join_pair);
	if (unique_domain > 0) {
		// The equalities cover a key: its row count is the composite NDV, not a proxy for it.
		cap = unique_domain;
	} else if (!stats.CanApplyCap(cap)) {
		// The cap is an FK/PK heuristic. Avoid applying it to broad fact-to-fact joins where the base row count is
		// much larger than any observed single-column join domain; in that case row count is a poor proxy for
		// composite NDV.
		return false;
	}
	auto &first_d = stats.first_distinct_count;
	if (cap <= 0 || first_d <= 0 || first_d > cap) {
		return false;
	}
	if (cap > 0 && first_d < cap && first_d > 0) {
		// Lower weak same-pair composite evidence to the FK/PK cap.
		target_selectivity = target_selectivity * CardinalityValue(first_d) / CardinalityValue(cap);
	}
	capped_join_pairs.insert(join_pair);
	pending_join_pairs.erase(it);
	return true;
}

bool CardinalityEstimator::CompositeJoinPairCaps::ApplyIncrement(CardinalityValue &target_selectivity,
                                                                 FilterInfoWithTotalDomains &edge,
                                                                 JoinRelationSet &scope) {
	if (edge.IsInnerEquality()) {
		auto target_pair = GetEqualityJoinPair(edge, estimator.set_manager);
		if (!target_pair) {
			return false;
		}
		if (!JoinRelationSet::IsSubset(scope, *target_pair) ||
		    !estimator.predicate_model.HasDirectCompositeEquality(*target_pair)) {
			return false;
		}
		if (capped_join_pairs.find(*target_pair) != capped_join_pairs.end()) {
			// The composite-key cap already accounts for same-pair equality predicates.
			return true;
		}
		return ApplyPair(target_selectivity, *target_pair);
	}

	return false;
}

bool CardinalityEstimator::CompositeJoinPairCaps::ApplyPending(CardinalityValue &target_selectivity,
                                                               JoinRelationSet &scope) {
	if (pending_join_pairs.empty()) {
		return false;
	}
	bool applied = false;
	for (auto it = pending_join_pairs.begin(); it != pending_join_pairs.end();) {
		// Applying the cap erases this entry.
		auto &join_pair = (it++)->first.get();
		D_ASSERT(capped_join_pairs.find(join_pair) == capped_join_pairs.end());
		if (!JoinRelationSet::IsSubset(scope, join_pair)) {
			continue;
		}
		D_ASSERT(estimator.predicate_model.HasDirectCompositeEquality(join_pair));
		applied = ApplyPair(target_selectivity, join_pair) || applied;
	}
	return applied;
}

enum class SelectivityEdgeKind : uint8_t {
	INNER_EQUIVALENCE,
	NON_EQUALITY_SELECTIVITY,
	REDUNDANT_TRANSITIVE_EQUALITY,
	OTHER
};

static bool EquivalenceGroupApplied(FilterInfoWithTotalDomains &edge, const unordered_set<idx_t> &applied_groups) {
	auto equality_class_index = edge.GetPredicate().GetEqualityClassIndex();
	return equality_class_index.IsValid() && applied_groups.count(equality_class_index.GetIndex()) > 0;
}

static SelectivityEdgeKind ClassifySelectivityEdge(FilterInfoWithTotalDomains &edge,
                                                   const unordered_set<idx_t> &applied_groups) {
	if (EquivalenceGroupApplied(edge, applied_groups)) {
		return SelectivityEdgeKind::REDUNDANT_TRANSITIVE_EQUALITY;
	}
	switch (edge.GetPredicate().GetPredicateClass()) {
	case JoinPredicateClass::INNER_EQUALITY:
		return SelectivityEdgeKind::INNER_EQUIVALENCE;
	case JoinPredicateClass::INNER_NON_EQUALITY:
		return SelectivityEdgeKind::NON_EQUALITY_SELECTIVITY;
	default:
		return SelectivityEdgeKind::OTHER;
	}
}

static bool CanIncrementExistingJoin(SelectivityEdgeKind kind) {
	return kind == SelectivityEdgeKind::INNER_EQUIVALENCE;
}

struct CardinalityEstimator::SelectivityState {
	explicit SelectivityState(CardinalityEstimator &estimator) : composite_caps(estimator) {
	}

	bool IsComplete(JoinRelationSet &set) const {
		return subgraphs.size() == 1 && subgraphs.at(0).relations &&
		       RefersToSameObject(*subgraphs.at(0).relations, set);
	}

	vector<SelectivitySubgraph> subgraphs;
	unordered_set<idx_t> applied_equivalence_groups;
	CompositeJoinPairCaps composite_caps;
	set<CardinalityValue> unused_edge_tdoms;
};

void CardinalityEstimator::ProcessSelectivityEdge(FilterInfoWithTotalDomains &edge, JoinRelationSet &requested_set,
                                                  SelectivityState &state) {
	auto edge_kind = ClassifySelectivityEdge(edge, state.applied_equivalence_groups);
	if (state.IsComplete(requested_set)) {
		auto &complete_subgraph = state.subgraphs.at(0);
		state.composite_caps.ApplyPending(complete_subgraph.selectivity, *complete_subgraph.relations);
		if (edge_kind == SelectivityEdgeKind::REDUNDANT_TRANSITIVE_EQUALITY) {
			return;
		}
		if (CanIncrementExistingJoin(edge_kind) &&
		    state.composite_caps.ApplyIncrement(complete_subgraph.selectivity, edge, *complete_subgraph.relations)) {
			return;
		}
		if (edge.HasReliableDistinctCount()) {
			state.unused_edge_tdoms.insert(edge.GetReliableDistinctCount());
		}
		return;
	}

	auto equality_class_index = edge.GetPredicate().GetEqualityClassIndex();
	if (equality_class_index.IsValid()) {
		state.applied_equivalence_groups.insert(equality_class_index.GetIndex());
	}
	state.composite_caps.Register(edge);

	auto edge_left_set = GetEdgeEndpoint(edge, set_manager, true);
	auto edge_right_set = GetEdgeEndpoint(edge, set_manager, false);
	if (!edge_left_set || !edge_right_set) {
		return;
	}

	auto subgraph_connections = SubgraphsConnectedByEdge(edge, state.subgraphs, set_manager);
	if (subgraph_connections.empty()) {
		CreateSelectivitySubgraph(edge, *edge_left_set, *edge_right_set, state);
	} else if (subgraph_connections.size() == 1) {
		ExtendSelectivitySubgraph(subgraph_connections.at(0), edge, *edge_left_set, *edge_right_set,
		                          CanIncrementExistingJoin(edge_kind), state);
	} else if (subgraph_connections.size() == 2) {
		MergeSelectivitySubgraphs(subgraph_connections, edge, state);
	}
}

void CardinalityEstimator::CreateSelectivitySubgraph(FilterInfoWithTotalDomains &edge, JoinRelationSet &edge_left_set,
                                                     JoinRelationSet &edge_right_set, SelectivityState &state) {
	auto left_subgraph = SelectivitySubgraph();
	auto selectivity = CalculateInnerJoinSelectivity(CardinalityValue(1), edge);
	left_subgraph.relations = &set_manager.Union(edge_left_set, edge_right_set);
	left_subgraph.selectivity = selectivity;
	state.composite_caps.ApplyPending(left_subgraph.selectivity, *left_subgraph.relations);
	state.subgraphs.push_back(left_subgraph);
}

void CardinalityEstimator::ExtendSelectivitySubgraph(idx_t subgraph_index, FilterInfoWithTotalDomains &edge,
                                                     JoinRelationSet &edge_left_set, JoinRelationSet &edge_right_set,
                                                     bool can_increment_existing_join, SelectivityState &state) {
	auto left_subgraph = &state.subgraphs.at(subgraph_index);
	auto right_subgraph = SelectivitySubgraph();
	right_subgraph.relations = &edge_right_set;
	if (JoinRelationSet::IsSubset(*left_subgraph->relations, *right_subgraph.relations)) {
		right_subgraph.relations = &edge_left_set;
	}

	if (JoinRelationSet::IsSubset(*left_subgraph->relations, edge_left_set) &&
	    JoinRelationSet::IsSubset(*left_subgraph->relations, edge_right_set)) {
		if (can_increment_existing_join) {
			state.composite_caps.ApplyIncrement(left_subgraph->selectivity, edge, *left_subgraph->relations);
		}
		state.composite_caps.ApplyPending(left_subgraph->selectivity, *left_subgraph->relations);
		return;
	}

	auto selectivity = CalculateInnerJoinSelectivity(left_subgraph->selectivity, edge);
	left_subgraph->relations = &set_manager.Union(*left_subgraph->relations, *right_subgraph.relations);
	left_subgraph->selectivity = selectivity;
	state.composite_caps.ApplyPending(left_subgraph->selectivity, *left_subgraph->relations);
}

void CardinalityEstimator::MergeSelectivitySubgraphs(const vector<idx_t> &subgraph_connections,
                                                     FilterInfoWithTotalDomains &edge, SelectivityState &state) {
	D_ASSERT(subgraph_connections.size() == 2);
	D_ASSERT(subgraph_connections.at(0) < subgraph_connections.at(1));
	auto subgraph_to_merge_into = &state.subgraphs.at(subgraph_connections.at(0));
	auto subgraph_to_delete = &state.subgraphs.at(subgraph_connections.at(1));
	auto selectivity =
	    CalculateInnerJoinSelectivity(subgraph_to_merge_into->selectivity * subgraph_to_delete->selectivity, edge);
	subgraph_to_merge_into->relations =
	    &set_manager.Union(*subgraph_to_merge_into->relations, *subgraph_to_delete->relations);
	subgraph_to_merge_into->selectivity = selectivity;
	state.composite_caps.ApplyPending(subgraph_to_merge_into->selectivity, *subgraph_to_merge_into->relations);
	subgraph_to_delete->relations = nullptr;
	auto remove_start = std::remove_if(state.subgraphs.begin(), state.subgraphs.end(),
	                                   [](SelectivitySubgraph &s) { return !s.relations; });
	state.subgraphs.erase(remove_start, state.subgraphs.end());
}

void CardinalityEstimator::MergeDisconnectedSelectivitySubgraphs(SelectivityState &state) {
	if (state.subgraphs.size() <= 1) {
		return;
	}
	auto final_subgraph = state.subgraphs.at(0);
	for (auto merge_with = state.subgraphs.begin() + 1; merge_with != state.subgraphs.end(); merge_with++) {
		D_ASSERT(final_subgraph.relations && merge_with->relations);
		final_subgraph.relations = &set_manager.Union(*final_subgraph.relations, *merge_with->relations);
		final_subgraph.selectivity *= merge_with->selectivity;
	}
	state.subgraphs.clear();
	state.subgraphs.push_back(final_subgraph);
}

CardinalityValue CardinalityEstimator::GetSelectivity(JoinRelationSet &set) {
	SelectivityState selectivity_state(*this);
	auto process_edge = [&](FilterInfoWithTotalDomains &edge) {
		auto &filter = edge.GetFilter();
		if (edge.GetPredicate().GetJoinType() == JoinType::INNER && filter.left_set != filter.right_set &&
		    IsFilterApplicable(filter, set) && !edge.residual) {
			ProcessSelectivityEdge(edge, set, selectivity_state);
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
	MergeDisconnectedSelectivitySubgraphs(selectivity_state);
	auto result =
	    selectivity_state.subgraphs.empty() ? CardinalityValue(1) : selectivity_state.subgraphs[0].selectivity;
	return result / CardinalityValue(1.0 + static_cast<double>(selectivity_state.unused_edge_tdoms.size()));
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
	return set.Contains(RelationIndex(binding.table_index.index));
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
		if (total->source == DistinctCountSource::CARDINALITY) {
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
			result = MaxValue(left_rows, left_rows * right_rows * boundary->selectivity);
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

//! One projection scope owns both boundary resolution and equality-domain reuse for its relation set.
class CardinalityEstimator::ColumnProjection {
public:
	ColumnProjection(CardinalityEstimator &estimator, JoinRelationSet &set) : estimator(estimator), set(set) {
	}

	optional<RelationColumnStats> ProjectColumn(ColumnBinding binding);
	optional<RelationStats> ProjectStats(const vector<ColumnBinding> &bindings);

private:
	void ResolveBoundary();
	optional<RelationColumnStats> ProjectInputColumn(ColumnBinding binding);
	void ApplyEqualityClass(const JoinEqualityClass &component, DistinctCount &domain);

private:
	CardinalityEstimator &estimator;
	JoinRelationSet &set;
	bool boundary_resolved = false;
	optional_ptr<const LogicalJoinStats> boundary;
	optional_ptr<JoinRelationSet> region;
	bool reuse_domains = false;
	//! An empty entry means the class is incomplete in this set. Entries never escape this projection.
	reference_map_t<const JoinEqualityClass, vector<CardinalityValue>> equality_domains;
};

void CardinalityEstimator::ColumnProjection::ResolveBoundary() {
	if (!boundary_resolved) {
		boundary = estimator.FindBoundary(set, region);
		boundary_resolved = true;
	}
}

optional<RelationColumnStats> CardinalityEstimator::ColumnProjection::ProjectInputColumn(ColumnBinding binding) {
	if (!boundary) {
		if (binding.table_index.index >= estimator.state->leaves.size()) {
			return {};
		}
		auto &columns = estimator.state->leaves[binding.table_index.index].columns;
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
		auto &input = ContainsBinding(*region, binding) ? *region : estimator.set_manager.Difference(set, *region);
		auto result = estimator.ProjectColumn(input, binding);
		if (result) {
			result->semi_anti_join_domain_evidence.Invalidate();
		}
		return result;
	}
	auto &op = boundary->op.get();
	auto &left = estimator.set_manager.Intersection(set, op.left_relations);
	auto &right = estimator.set_manager.Intersection(set, op.right_relations);
	bool on_left = ContainsBinding(left, binding);
	if (!on_left && op.type == JoinOrderOperatorType::MARK) {
		auto rhs = op.right_relations.get().relations[0];
		if (binding.table_index.index == rhs.index &&
		    binding.column_index.GetIndex() == estimator.state->leaves[rhs.index].columns.size()) {
			return RelationColumnStats(binding, DistinctCount(3, DistinctCountSource::CARDINALITY), Identifier("mark"));
		}
		return {};
	}
	if (!on_left && op.type != JoinOrderOperatorType::LEFT) {
		return {};
	}
	auto result = estimator.ProjectColumn(on_left ? left : right, binding);
	if (!result) {
		return {};
	}
	auto left_rows = estimator.state->cardinalities.at(left).cardinality;
	CardinalityValue join_rows;
	if (op.type == JoinOrderOperatorType::LEFT) {
		join_rows = MaxValue(left_rows,
		                     left_rows * estimator.state->cardinalities.at(right).cardinality * boundary->selectivity);
		result->semi_anti_join_domain_evidence.Invalidate();
	} else if (op.type == JoinOrderOperatorType::MARK) {
		join_rows = left_rows;
	} else {
		vector<SemiAntiJoinDomain> domains;
		auto estimate = estimator.EstimateMembership(*boundary, left, right, domains);
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
	auto post_selectivity = estimator.GetJoinSelectivity(set, left, right);
	result->current_domain = RelationStatisticsHelper::EstimateCurrentDomain(result->current_domain, join_rows,
	                                                                         join_rows * post_selectivity);
	return result;
}

void CardinalityEstimator::ColumnProjection::ApplyEqualityClass(const JoinEqualityClass &component,
                                                                DistinctCount &domain) {
	auto current = domain.distinct_count;
	auto cached = reuse_domains ? equality_domains.find(component) : equality_domains.end();
	if (cached != equality_domains.end()) {
		if (cached->second.empty()) {
			return;
		}
		// Preserve the original seed and member order; rounded cardinality comparisons cannot be reassociated.
		for (auto &input : cached->second) {
			current = MinValue(current, input);
		}
	} else {
		bool complete = true;
		vector<CardinalityValue> domains;
		for (auto &other : component.columns) {
			if (!ContainsBinding(set, other)) {
				complete = false;
				break;
			}
			auto input = ProjectInputColumn(other);
			if (!input) {
				complete = false;
				break;
			}
			current = MinValue(current, input->current_domain.distinct_count);
			// Repeating the same operand is idempotent; retain the order of all distinct representations.
			if (reuse_domains &&
			    (domains.empty() || !domains.back().HasSameRepresentation(input->current_domain.distinct_count))) {
				domains.push_back(input->current_domain.distinct_count);
			}
		}
		for (auto &edge : component.edges) {
			if (!complete || !estimator.IsFilterApplicable(edge.predicate.get().GetFilter(), set)) {
				complete = false;
				break;
			}
		}
		if (reuse_domains) {
			equality_domains.emplace(component, complete ? std::move(domains) : vector<CardinalityValue>());
		}
		if (!complete) {
			return;
		}
	}
	domain = DistinctCount(current, DistinctCountSource::CARDINALITY);
}

optional<RelationColumnStats> CardinalityEstimator::ColumnProjection::ProjectColumn(ColumnBinding binding) {
	if (!ContainsBinding(set, binding)) {
		return {};
	}
	auto rows = estimator.EstimateCardinality(set);
	auto cached = estimator.state->projected_columns.find(set);
	if (cached != estimator.state->projected_columns.end()) {
		auto column = cached->second.find(binding);
		if (column != cached->second.end()) {
			return column->second;
		}
	}
	ResolveBoundary();
	auto result = ProjectInputColumn(binding);
	if (!result) {
		return {};
	}
	for (auto &component : estimator.predicate_model.GetEqualityClasses()) {
		if (component.columns.count(binding)) {
			ApplyEqualityClass(component, result->current_domain);
		}
	}
	result->current_domain.distinct_count =
	    MinValue(MinValue(result->current_domain.distinct_count, result->total_domain.distinct_count), rows);
	if (boundary) {
		estimator.state->projected_columns[set].emplace(binding, *result);
	}
	return result;
}

optional<RelationStats> CardinalityEstimator::ColumnProjection::ProjectStats(const vector<ColumnBinding> &bindings) {
	RelationStats result;
	result.cardinality = estimator.EstimateCardinality(set);
	auto &estimate = estimator.state->cardinalities.at(set);
	result.max_cardinality = estimate.maximum;
	result.row_retention = estimate.row_retention;
	// Scalar requests keep the direct, allocation-free reduction.
	reuse_domains = bindings.size() > 1;
	for (auto &binding : bindings) {
		auto column = ProjectColumn(binding);
		if (!column) {
			return {};
		}
		result.columns.push_back(*column);
	}
	result.stats_initialized = true;
	result.Verify(bindings);
	return result;
}

optional<RelationColumnStats> CardinalityEstimator::ProjectColumn(JoinRelationSet &set, ColumnBinding binding) {
	return ColumnProjection(*this, set).ProjectColumn(binding);
}

optional<RelationStats> CardinalityEstimator::ProjectStats(JoinRelationSet &set,
                                                           const vector<ColumnBinding> &bindings) {
	return ColumnProjection(*this, set).ProjectStats(bindings);
}

static CardinalityValue GetResidualOnSelectivity(const Expression &expression) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		auto &value = expression.Cast<BoundConstantExpression>().GetValue();
		return CardinalityValue(!value.IsNull() && value.GetValue<bool>() ? 1 : 0);
	}
	return CardinalityValue(RelationStatisticsHelper::DEFAULT_SELECTIVITY);
}

CardinalityValue CardinalityEstimator::GetLeftJoinSelectivity(const JoinOrderOperator &join_operator) {
	CardinalityValue selectivity;
	unordered_set<idx_t> equality_groups;
	unordered_set<idx_t> accounted_predicates;
	reference_map_t<JoinRelationSet, CardinalityValue> pair_selectivities;
	reference_map_t<JoinRelationSet, CompositeJoinPairStats> pair_stats;
	for (auto &edge : state->edges) {
		auto &predicate = edge.GetPredicate();
		auto source = predicate.GetFilter().source_operator_index;
		if (!source.IsValid() || source.GetIndex() != join_operator.index) {
			continue;
		}
		accounted_predicates.insert(predicate.GetFilter().filter_index);
		auto group = predicate.GetFilter().left_equality_group;
		if (group.IsValid() && !equality_groups.insert(group.GetIndex()).second) {
			continue;
		}
		auto factor = CalculateInnerJoinSelectivity(CardinalityValue(1), edge);
		auto pair = predicate.GetCompositeJoinPair();
		if (pair) {
			pair_stats[*pair].RegisterDistinctCount(edge.GetDistinctCount());
			auto entry = pair_selectivities.emplace(*pair, CardinalityValue(1)).first;
			entry->second *= factor;
		} else {
			selectivity *= factor;
		}
	}
	for (auto &entry : pair_selectivities) {
		auto cap = GetJoinPairCap(entry.first);
		selectivity *=
		    pair_stats.at(entry.first).CanApplyCap(cap) ? CardinalityValue(1) / CardinalityValue(cap) : entry.second;
	}
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

static bool IsSelectivityComparison(const JoinPredicate &predicate) {
	switch (predicate.GetComparisonType()) {
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_DISTINCT_FROM:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		break;
	default:
		return false;
	}
	auto predicate_class = predicate.GetPredicateClass();
	return predicate_class == JoinPredicateClass::INNER_NON_EQUALITY ||
	       predicate_class == JoinPredicateClass::LEFT_JOIN;
}

static bool SortEdges(const FilterInfoWithTotalDomains &a, const FilterInfoWithTotalDomains &b) {
	// Broad comparison domains must not displace equality edges.
	auto a_comparison = IsSelectivityComparison(a.GetPredicate());
	auto b_comparison = IsSelectivityComparison(b.GetPredicate());
	if (a_comparison != b_comparison) {
		return b_comparison;
	}
	return a.GetDistinctCount() > b.GetDistinctCount();
}

void CardinalityEstimator::Initialize(vector<RelationStats> relations,
                                      const vector<unique_ptr<JoinOrderOperator>> &operators,
                                      const join_column_sources_t &sources) {
	D_ASSERT(state->leaves.empty());
	state->leaves = std::move(relations);
	// Region-local single-input filters commute to that input. Capture their effect once,
	// while expressions and column bindings still describe the original input.
	std::map<idx_t, vector<unique_ptr<Expression>>> leaf_expressions;
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
		state->cardinalities.emplace(set_manager.GetJoinRelation(RelationIndex(i)),
		                             RegionEstimate {cardinality, stats.max_cardinality, stats.row_retention});
		UpdateTotalDomains(RelationIndex(i), stats, sources);
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
	RestrictFilterDomains(state->leaves);
	for (auto &domain : state->relation_set_stats) {
		for (auto &predicate : domain.predicates) {
			state->edges.emplace_back(predicate, domain);
		}
	}
	std::stable_sort(state->edges.begin(), state->edges.end(), SortEdges);
	state->InitializeEdgeIndex();
	for (auto &op : operators) {
		D_ASSERT(op->index == state->operators.size());
		state->operators.push_back(*op);
		if (op->type == JoinOrderOperatorType::INNER || op->type == JoinOrderOperatorType::CROSS_PRODUCT) {
			continue;
		}
		auto selectivity = op->type == JoinOrderOperatorType::LEFT ? GetLeftJoinSelectivity(*op) : CardinalityValue(1);
		state->boundaries.emplace_back(*op, set_manager.Union(op->left_relations, op->right_relations), selectivity);
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
			auto left = predicate.GetEqualityBinding(true);
			auto right = predicate.GetEqualityBinding(false);
			if (ContainsBinding(op->right_relations, left) && ContainsBinding(op->left_relations, right)) {
				std::swap(left, right);
			}
			if (!ContainsBinding(op->left_relations, left) || !ContainsBinding(op->right_relations, right)) {
				boundary.has_residual = true;
				continue;
			}
			auto key = make_pair(left, right);
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

void CardinalityEstimator::RestrictFilterDomains(const vector<RelationStats> &relation_stats) {
	for (auto &equivalence : state->relation_set_stats) {
		bool all_filtered = !equivalence.predicates.empty();
		for (auto &predicate : equivalence.predicates) {
			all_filtered &= predicate.get().IsEquivalencePredicate();
		}
		idx_t retained_domain = 0;
		for (auto &binding : equivalence.equivalent_relations) {
			if (!all_filtered || binding.table_index.index >= relation_stats.size() ||
			    binding.column_index.GetIndex() >= relation_stats[binding.table_index.index].columns.size()) {
				all_filtered = false;
				break;
			}
			auto &column = relation_stats[binding.table_index.index].columns[binding.column_index.GetIndex()];
			auto bound = column.semi_anti_join_domain_evidence.GetDomainBound();
			// Unknown-NDV filters need their scan selectivities refined before shrinking the join domain.
			if (!bound.IsValid() || column.total_domain.source == DistinctCountSource::CARDINALITY) {
				all_filtered = false;
				break;
			}
			retained_domain = MaxValue(retained_domain, bound.GetIndex());
		}
		if (all_filtered) {
			equivalence.domain_estimate.Restrict(retained_domain);
		}
	}
}

} // namespace duckdb
