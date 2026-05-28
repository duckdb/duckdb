#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/query_graph_manager.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/storage/data_table.hpp"

#include <math.h>

namespace duckdb {

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
	for (const RelationsSetToStats &r2tdom : relation_set_stats) {
		auto &i_set = r2tdom.equivalent_relations;
		if (i_set.find(binding) != i_set.end()) {
			// found an equivalent filter
			return;
		}
	}

	auto key = ColumnBinding(binding.table_index, binding.column_index);
	RelationsSetToStats new_r2tdom(column_binding_set_t({key}));

	relation_set_stats.emplace_back(new_r2tdom);
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

static bool IsJoinOrFilter(const FilterInfo &filter) {
	return filter.filter && filter.filter->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	       filter.filter->GetExpressionType() == ExpressionType::CONJUNCTION_OR && filter.set.get().count >= 2;
}

void CardinalityEstimator::InitEquivalentRelations() {
	relation_set_stats.clear();
	or_filters.clear();

	for (auto predicate_ref : predicate_model.GetPredicates()) {
		auto &filter = predicate_ref.get().GetFilter();
		if (IsJoinOrFilter(filter)) {
			or_filters.push_back(filter);
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
		relation_set_stats.emplace_back(equality_class.columns);
		for (auto &edge : equality_class.edges) {
			relation_set_stats.back().predicates.push_back(edge.predicate);
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
		relation_set_stats.emplace_back(domain_group);
		relation_set_stats.back().predicates.push_back(predicate);
	}
	RemoveEmptyTotalDomains();
}

void CardinalityEstimator::RemoveEmptyTotalDomains() {
	auto remove_start =
	    std::remove_if(relation_set_stats.begin(), relation_set_stats.end(),
	                   [](RelationsSetToStats &r_2_tdom) { return r_2_tdom.equivalent_relations.empty(); });
	relation_set_stats.erase(remove_start, relation_set_stats.end());
}

double CardinalityEstimator::GetNumerator(JoinRelationSet &set) {
	double numerator = 1;
	for (idx_t i = 0; i < set.count; i++) {
		auto &single_node_set = set_manager.GetJoinRelation(set.relations[i]);
		auto entry = relation_set_2_cardinality.find(single_node_set);
		D_ASSERT(entry != relation_set_2_cardinality.end());
		if (entry == relation_set_2_cardinality.end()) {
			continue;
		}
		auto &card_helper = entry->second;
		numerator *= card_helper.cardinality_before_filters == 0 ? 1 : card_helper.cardinality_before_filters;
	}
	return numerator;
}

vector<FilterInfoWithTotalDomains> GetEdges(vector<RelationsSetToStats> &relations_to_tdom,
                                            JoinRelationSet &requested_set) {
	vector<FilterInfoWithTotalDomains> res;
	for (auto &relation_2_tdom : relations_to_tdom) {
		for (auto predicate_ref : relation_2_tdom.predicates) {
			auto &predicate = predicate_ref.get();
			auto &filter = predicate.GetFilter();
			if (JoinRelationSet::IsSubset(requested_set, filter.set.get()) && filter.left_set != filter.right_set) {
				FilterInfoWithTotalDomains new_edge(predicate, relation_2_tdom);
				res.push_back(new_edge);
			}
		}
	}
	return res;
}

static optional_ptr<JoinRelationSet> GetEdgeEndpoint(FilterInfoWithTotalDomains &edge,
                                                     JoinRelationSetManager &set_manager, bool left) {
	auto &filter = edge.GetFilter();
	if (filter.filter && BoundComparisonExpression::IsComparison(*filter.filter)) {
		auto &binding = edge.GetPredicate().GetStatsBinding(left);
		if (binding.table_index.IsValid()) {
			return &set_manager.GetJoinRelation(RelationIndex(binding.table_index.index));
		}
	}
	return left ? edge.GetPredicate().GetLeftSetOptional() : edge.GetPredicate().GetRightSetOptional();
}

bool EdgeConnects(FilterInfoWithTotalDomains &edge, Subgraph2Denominator &subgraph,
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

vector<idx_t> SubgraphsConnectedByEdge(FilterInfoWithTotalDomains &edge, vector<Subgraph2Denominator> &subgraphs,
                                       JoinRelationSetManager &set_manager) {
	vector<idx_t> res;
	if (subgraphs.empty()) {
		return res;
	} else {
		// check the combinations of subgraphs and see if the edge connects two of them,
		// if so, return the indexes of the two subgraphs within the vector
		for (idx_t outer = 0; outer != subgraphs.size(); outer++) {
			// check if the edge connects two subgraphs.
			for (idx_t inner = outer + 1; inner != subgraphs.size(); inner++) {
				if (EdgeConnects(edge, subgraphs.at(outer), set_manager) &&
				    EdgeConnects(edge, subgraphs.at(inner), set_manager)) {
					// order is important because we will delete the inner subgraph later
					res.push_back(outer);
					res.push_back(inner);
					return res;
				}
			}
			// if the edge does not connect two subgraphs, see if the edge connects with just outer
			// merge subgraph.at(outer) with the RelationSet(s) that edge connects
			if (EdgeConnects(edge, subgraphs.at(outer), set_manager)) {
				res.push_back(outer);
				return res;
			}
		}
	}
	// this edge connects only the relations it connects. Return an empty result so a new subgraph is created.
	return res;
}

JoinRelationSet &CardinalityEstimator::UpdateNumeratorRelations(Subgraph2Denominator left, Subgraph2Denominator right,
                                                                FilterInfoWithTotalDomains &filter) {
	auto &predicate = filter.GetPredicate();
	switch (predicate.GetJoinType()) {
	case JoinType::LEFT:
	case JoinType::SEMI:
	case JoinType::ANTI: {
		if (JoinRelationSet::IsSubset(*left.relations, predicate.GetLeftSet()) &&
		    JoinRelationSet::IsSubset(*right.relations, predicate.GetRightSet())) {
			return *left.numerator_relations;
		}
		return *right.numerator_relations;
	}
	default:
		// cross product or inner join
		return set_manager.Union(*left.numerator_relations, *right.numerator_relations);
	}
}

// Apply the denominator multiplier for a given comparison type and effective distinct count.
static double ApplyComparisonRatio(double base_denom, ExpressionType comparison_type, double effective_d) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return base_denom * effective_d;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_DISTINCT_FROM:
		// Assume this blows up, but use the tdom to bound it a bit.
		return base_denom * pow(effective_d, 2.0 / 3.0);
	default:
		return base_denom;
	}
}

double CardinalityEstimator::CalculateInnerJoinDenom(double base_denom, FilterInfoWithTotalDomains &filter) {
	auto effective_d = filter.GetDistinctCount();
	auto comparison_type = filter.GetComparisonType();
	if (comparison_type == ExpressionType::INVALID) {
		return base_denom * effective_d;
	}
	return ApplyComparisonRatio(base_denom, comparison_type, effective_d);
}

double CardinalityEstimator::CalculateLeftJoinDenom(Subgraph2Denominator &left, Subgraph2Denominator &right,
                                                    FilterInfoWithTotalDomains &filter) {
	auto &predicate = filter.GetPredicate();
	if (JoinRelationSet::IsSubset(*left.relations, predicate.GetLeftSet()) &&
	    JoinRelationSet::IsSubset(*right.relations, predicate.GetRightSet())) {
		return left.denom;
	}
	return right.denom;
}

double CardinalityEstimator::CalculateSemiAntiJoinDenom(double base_denom, Subgraph2Denominator &left,
                                                        Subgraph2Denominator &right,
                                                        FilterInfoWithTotalDomains &filter) {
	auto &predicate = filter.GetPredicate();
	if (JoinRelationSet::IsSubset(*left.relations, predicate.GetLeftSet()) &&
	    JoinRelationSet::IsSubset(*right.relations, predicate.GetRightSet())) {
		return left.denom * DEFAULT_SEMI_ANTI_SELECTIVITY;
	}
	return right.denom * DEFAULT_SEMI_ANTI_SELECTIVITY;
}

// Given two subgraphs, compute the updated denominator for the join between them.
double CardinalityEstimator::CalculateUpdatedDenom(Subgraph2Denominator left, Subgraph2Denominator right,
                                                   FilterInfoWithTotalDomains &filter) {
	double base_denom = left.denom * right.denom;
	switch (filter.GetPredicate().GetJoinType()) {
	case JoinType::LEFT:
		return CalculateLeftJoinDenom(left, right, filter);
	case JoinType::INNER:
		return CalculateInnerJoinDenom(base_denom, filter);
	case JoinType::SEMI:
	case JoinType::ANTI:
		return CalculateSemiAntiJoinDenom(base_denom, left, right, filter);
	default:
		// Cross product: no join condition reduces the denominator.
		return base_denom;
	}
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

double CardinalityEstimator::GetJoinPairCap(JoinRelationSet &join_pair) {
	D_ASSERT(join_pair.count == 2);
	double cap = NumericLimits<double>::Maximum();
	for (idx_t relation_index = 0; relation_index < join_pair.count; relation_index++) {
		auto &single_relation = set_manager.GetJoinRelation(join_pair.relations[relation_index]);
		auto cardinality = GetNumerator(single_relation);
		if (cardinality <= 0) {
			return 0;
		}
		cap = MinValue(cap, cardinality);
	}
	return cap == NumericLimits<double>::Maximum() ? 0 : cap;
}

bool CardinalityEstimator::ApplyJoinPairCap(double &target_denom, JoinRelationSet &join_pair,
                                            reference_map_t<JoinRelationSet, CompositeJoinPairStats> &join_pair_stats,
                                            reference_set_t<JoinRelationSet> &capped_join_pairs) {
	if (capped_join_pairs.find(join_pair) != capped_join_pairs.end()) {
		return false;
	}
	const auto it = join_pair_stats.find(join_pair);
	if (it == join_pair_stats.end()) {
		return false;
	}
	auto &stats = it->second;
	auto cap = GetJoinPairCap(join_pair);
	// The cap is an FK/PK heuristic. Avoid applying it to broad fact-to-fact joins where the base row count is much
	// larger than any observed single-column join domain; in that case row count is a poor proxy for composite NDV.
	if (!stats.CanApplyCap(cap)) {
		return false;
	}
	auto &first_d = stats.first_distinct_count;
	if (cap <= 0 || first_d <= 0 || first_d > cap) {
		return false;
	}
	if (cap > 0 && first_d < cap && first_d > 0) {
		// Raise weak same-pair composite evidence to the FK/PK denominator floor.
		target_denom = target_denom / first_d * cap;
		first_d = cap;
	}
	capped_join_pairs.insert(join_pair);
	return true;
}

bool CardinalityEstimator::ApplyJoinIncrement(double &target_denom, FilterInfoWithTotalDomains &edge,
                                              reference_map_t<JoinRelationSet, CompositeJoinPairStats> &join_pair_stats,
                                              reference_set_t<JoinRelationSet> &capped_join_pairs,
                                              JoinRelationSet &scope, optional_ptr<JoinRelationSet> join_pair) {
	if (edge.IsInnerEquality()) {
		auto target_pair = join_pair;
		if (!target_pair) {
			target_pair = GetEqualityJoinPair(edge, set_manager);
		}
		if (!target_pair) {
			return false;
		}
		if (!JoinRelationSet::IsSubset(scope, *target_pair) ||
		    !predicate_model.HasDirectCompositeEquality(*target_pair)) {
			return false;
		}
		if (capped_join_pairs.find(*target_pair) != capped_join_pairs.end()) {
			// The composite-key cap already accounts for same-pair equality predicates.
			return true;
		}
		return ApplyJoinPairCap(target_denom, *target_pair, join_pair_stats, capped_join_pairs);
	}

	if (edge.GetPredicate().GetJoinType() == JoinType::LEFT) {
		// LEFT joins preserve the LHS cardinality in this estimator. Additional LEFT conditions
		// should not reduce the output or add unused-edge penalties.
		return true;
	}

	return false;
}

bool CardinalityEstimator::ApplyCompositeJoinPairCaps(
    double &target_denom, JoinRelationSet &scope,
    reference_map_t<JoinRelationSet, CompositeJoinPairStats> &join_pair_stats,
    reference_set_t<JoinRelationSet> &capped_join_pairs) {
	bool applied = false;
	for (auto &entry : join_pair_stats) {
		auto &join_pair = entry.first.get();
		if (capped_join_pairs.find(join_pair) != capped_join_pairs.end()) {
			continue;
		}
		if (!JoinRelationSet::IsSubset(scope, join_pair)) {
			continue;
		}
		if (!predicate_model.HasDirectCompositeEquality(join_pair)) {
			continue;
		}
		applied = ApplyJoinPairCap(target_denom, join_pair, join_pair_stats, capped_join_pairs) || applied;
	}
	return applied;
}

enum class DenominatorEdgeKind : uint8_t {
	INNER_EQUIVALENCE,
	LEFT_JOIN,
	SEMI_ANTI_JOIN,
	NON_EQUALITY_SELECTIVITY,
	REDUNDANT_TRANSITIVE_EQUALITY,
	OTHER
};

static bool EquivalenceGroupApplied(FilterInfoWithTotalDomains &edge, const unordered_set<idx_t> &applied_groups) {
	auto equality_class_index = edge.GetPredicate().GetEqualityClassIndex();
	return equality_class_index.IsValid() && applied_groups.count(equality_class_index.GetIndex()) > 0;
}

static DenominatorEdgeKind ClassifyDenominatorEdge(FilterInfoWithTotalDomains &edge,
                                                   const unordered_set<idx_t> &applied_groups) {
	if (EquivalenceGroupApplied(edge, applied_groups)) {
		return DenominatorEdgeKind::REDUNDANT_TRANSITIVE_EQUALITY;
	}
	switch (edge.GetPredicate().GetPredicateClass()) {
	case JoinPredicateClass::INNER_EQUALITY:
		return DenominatorEdgeKind::INNER_EQUIVALENCE;
	case JoinPredicateClass::INNER_NON_EQUALITY:
		return DenominatorEdgeKind::NON_EQUALITY_SELECTIVITY;
	case JoinPredicateClass::LEFT_JOIN:
		return DenominatorEdgeKind::LEFT_JOIN;
	case JoinPredicateClass::SEMI_ANTI_JOIN:
		return DenominatorEdgeKind::SEMI_ANTI_JOIN;
	default:
		return DenominatorEdgeKind::OTHER;
	}
}

static bool CanIncrementExistingJoin(DenominatorEdgeKind kind) {
	return kind == DenominatorEdgeKind::INNER_EQUIVALENCE || kind == DenominatorEdgeKind::LEFT_JOIN;
}

static void RegisterEqualityJoinPair(FilterInfoWithTotalDomains &edge, JoinRelationSetManager &set_manager,
                                     reference_map_t<JoinRelationSet, CompositeJoinPairStats> &join_pair_stats) {
	if (!edge.IsInnerEquality()) {
		return;
	}
	auto join_pair = GetEqualityJoinPair(edge, set_manager);
	if (!join_pair) {
		return;
	}
	join_pair_stats[*join_pair].RegisterDistinctCount(edge.GetDistinctCount());
}

struct DenominatorState {
	vector<Subgraph2Denominator> subgraphs;
	unordered_set<idx_t> applied_equivalence_groups;
	reference_map_t<JoinRelationSet, CompositeJoinPairStats> join_pair_stats;
	reference_set_t<JoinRelationSet> capped_join_pairs;
	unordered_set<idx_t> unused_edge_tdoms;
};

static bool DenominatorSubgraphComplete(DenominatorState &state, JoinRelationSet &set) {
	return state.subgraphs.size() == 1 && state.subgraphs.at(0).relations &&
	       RefersToSameObject(*state.subgraphs.at(0).relations, set);
}

void CardinalityEstimator::ProcessDenominatorEdge(FilterInfoWithTotalDomains &edge, JoinRelationSet &requested_set,
                                                  DenominatorState &state) {
	auto edge_kind = ClassifyDenominatorEdge(edge, state.applied_equivalence_groups);
	if (DenominatorSubgraphComplete(state, requested_set)) {
		auto &complete_subgraph = state.subgraphs.at(0);
		ApplyCompositeJoinPairCaps(complete_subgraph.denom, *complete_subgraph.relations, state.join_pair_stats,
		                           state.capped_join_pairs);
		if (edge_kind == DenominatorEdgeKind::REDUNDANT_TRANSITIVE_EQUALITY) {
			return;
		}
		if (CanIncrementExistingJoin(edge_kind) &&
		    ApplyJoinIncrement(complete_subgraph.denom, edge, state.join_pair_stats, state.capped_join_pairs,
		                       *complete_subgraph.relations)) {
			return;
		}
		if (edge.has_distinct_count_hll) {
			state.unused_edge_tdoms.insert(edge.distinct_count_hll);
		}
		return;
	}

	auto equality_class_index = edge.GetPredicate().GetEqualityClassIndex();
	if (equality_class_index.IsValid()) {
		state.applied_equivalence_groups.insert(equality_class_index.GetIndex());
	}
	RegisterEqualityJoinPair(edge, set_manager, state.join_pair_stats);

	auto edge_left_set = GetEdgeEndpoint(edge, set_manager, true);
	auto edge_right_set = GetEdgeEndpoint(edge, set_manager, false);
	if (!edge_left_set || !edge_right_set) {
		return;
	}

	auto subgraph_connections = SubgraphsConnectedByEdge(edge, state.subgraphs, set_manager);
	if (subgraph_connections.empty()) {
		CreateDenominatorSubgraph(edge, *edge_left_set, *edge_right_set, state);
	} else if (subgraph_connections.size() == 1) {
		ExtendDenominatorSubgraph(subgraph_connections.at(0), edge, *edge_left_set, *edge_right_set,
		                          CanIncrementExistingJoin(edge_kind), state);
	} else if (subgraph_connections.size() == 2) {
		MergeDenominatorSubgraphs(subgraph_connections, edge, state);
	}
}

void CardinalityEstimator::CreateDenominatorSubgraph(FilterInfoWithTotalDomains &edge, JoinRelationSet &edge_left_set,
                                                     JoinRelationSet &edge_right_set, DenominatorState &state) {
	auto left_subgraph = Subgraph2Denominator();
	auto right_subgraph = Subgraph2Denominator();
	left_subgraph.relations = &edge_left_set;
	left_subgraph.numerator_relations = &edge_left_set;
	right_subgraph.relations = &edge_right_set;
	right_subgraph.numerator_relations = &edge_right_set;
	left_subgraph.numerator_relations = &UpdateNumeratorRelations(left_subgraph, right_subgraph, edge);
	left_subgraph.relations = &set_manager.Union(edge_left_set, edge_right_set);
	left_subgraph.denom = CalculateUpdatedDenom(left_subgraph, right_subgraph, edge);
	ApplyCompositeJoinPairCaps(left_subgraph.denom, *left_subgraph.relations, state.join_pair_stats,
	                           state.capped_join_pairs);
	state.subgraphs.push_back(left_subgraph);
}

void CardinalityEstimator::ExtendDenominatorSubgraph(idx_t subgraph_index, FilterInfoWithTotalDomains &edge,
                                                     JoinRelationSet &edge_left_set, JoinRelationSet &edge_right_set,
                                                     bool can_increment_existing_join, DenominatorState &state) {
	auto left_subgraph = &state.subgraphs.at(subgraph_index);
	auto right_subgraph = Subgraph2Denominator();
	right_subgraph.relations = &edge_right_set;
	right_subgraph.numerator_relations = &edge_right_set;
	if (JoinRelationSet::IsSubset(*left_subgraph->relations, *right_subgraph.relations)) {
		right_subgraph.relations = &edge_left_set;
		right_subgraph.numerator_relations = &edge_left_set;
	}

	if (JoinRelationSet::IsSubset(*left_subgraph->relations, edge_left_set) &&
	    JoinRelationSet::IsSubset(*left_subgraph->relations, edge_right_set)) {
		if (can_increment_existing_join) {
			ApplyJoinIncrement(left_subgraph->denom, edge, state.join_pair_stats, state.capped_join_pairs,
			                   *left_subgraph->relations);
		}
		ApplyCompositeJoinPairCaps(left_subgraph->denom, *left_subgraph->relations, state.join_pair_stats,
		                           state.capped_join_pairs);
		return;
	}

	left_subgraph->numerator_relations = &UpdateNumeratorRelations(*left_subgraph, right_subgraph, edge);
	left_subgraph->relations = &set_manager.Union(*left_subgraph->relations, *right_subgraph.relations);
	left_subgraph->denom = CalculateUpdatedDenom(*left_subgraph, right_subgraph, edge);
	ApplyCompositeJoinPairCaps(left_subgraph->denom, *left_subgraph->relations, state.join_pair_stats,
	                           state.capped_join_pairs);
}

void CardinalityEstimator::MergeDenominatorSubgraphs(const vector<idx_t> &subgraph_connections,
                                                     FilterInfoWithTotalDomains &edge, DenominatorState &state) {
	D_ASSERT(subgraph_connections.size() == 2);
	D_ASSERT(subgraph_connections.at(0) < subgraph_connections.at(1));
	auto subgraph_to_merge_into = &state.subgraphs.at(subgraph_connections.at(0));
	auto subgraph_to_delete = &state.subgraphs.at(subgraph_connections.at(1));
	subgraph_to_merge_into->relations =
	    &set_manager.Union(*subgraph_to_merge_into->relations, *subgraph_to_delete->relations);
	subgraph_to_merge_into->numerator_relations =
	    &UpdateNumeratorRelations(*subgraph_to_merge_into, *subgraph_to_delete, edge);
	subgraph_to_merge_into->denom = CalculateUpdatedDenom(*subgraph_to_merge_into, *subgraph_to_delete, edge);
	ApplyCompositeJoinPairCaps(subgraph_to_merge_into->denom, *subgraph_to_merge_into->relations, state.join_pair_stats,
	                           state.capped_join_pairs);
	subgraph_to_delete->relations = nullptr;
	auto remove_start = std::remove_if(state.subgraphs.begin(), state.subgraphs.end(),
	                                   [](Subgraph2Denominator &s) { return !s.relations; });
	state.subgraphs.erase(remove_start, state.subgraphs.end());
}

void CardinalityEstimator::MergeDisconnectedDenominatorSubgraphs(DenominatorState &state) {
	if (state.subgraphs.size() <= 1) {
		return;
	}
	auto final_subgraph = state.subgraphs.at(0);
	for (auto merge_with = state.subgraphs.begin() + 1; merge_with != state.subgraphs.end(); merge_with++) {
		D_ASSERT(final_subgraph.relations && merge_with->relations);
		final_subgraph.relations = &set_manager.Union(*final_subgraph.relations, *merge_with->relations);
		D_ASSERT(final_subgraph.numerator_relations && merge_with->numerator_relations);
		final_subgraph.numerator_relations =
		    &set_manager.Union(*final_subgraph.numerator_relations, *merge_with->numerator_relations);
		final_subgraph.denom *= merge_with->denom;
	}
	state.subgraphs.clear();
	state.subgraphs.push_back(final_subgraph);
}

void CardinalityEstimator::AddCrossProductRelations(JoinRelationSet &set, DenominatorState &state) {
	if (state.subgraphs.empty()) {
		return;
	}
	auto &returning_subgraph = state.subgraphs.at(0);
	if (returning_subgraph.relations->count == set.count) {
		return;
	}
	for (idx_t rel_index = 0; rel_index < set.count; rel_index++) {
		auto relation_id = set.relations[rel_index];
		auto &rel = set_manager.GetJoinRelation(relation_id);
		if (!JoinRelationSet::IsSubset(*returning_subgraph.relations, rel)) {
			returning_subgraph.numerator_relations = &set_manager.Union(*returning_subgraph.numerator_relations, rel);
			returning_subgraph.relations = &set_manager.Union(*returning_subgraph.relations, rel);
		}
	}
}

DenomInfo CardinalityEstimator::CreateDenominatorResult(JoinRelationSet &set, DenominatorState &state) {
	auto denom_multiplier = 1.0 + static_cast<double>(state.unused_edge_tdoms.size());
	MergeDisconnectedDenominatorSubgraphs(state);
	AddCrossProductRelations(set, state);

	if (state.subgraphs.empty() || state.subgraphs.at(0).denom == 0) {
		return DenomInfo(set, 1);
	}
	return DenomInfo(*state.subgraphs.at(0).numerator_relations, state.subgraphs.at(0).denom * denom_multiplier);
}

DenomInfo CardinalityEstimator::GetDenominator(JoinRelationSet &set) {
	DenominatorState state;
	auto edges = GetEdges(relation_set_stats, set);
	for (auto &edge : edges) {
		ProcessDenominatorEdge(edge, set, state);
	}
	return CreateDenominatorResult(set, state);
}

// Cardinality is calculated using logic based on
// https://blobs.duckdb.org/papers/tom-ebergen-msc-thesis-join-order-optimization-with-almost-no-statistics.pdf
//
// The estimator starts with base relation cardinalities and divides by a denominator assembled from predicate domain
// groups. INNER equality predicates use transitive equality classes, composite same-pair equalities can apply an FK/PK
// cap, disconnected predicate subgraphs are merged by cross product, and LEFT/SEMI/ANTI joins adjust the numerator side
// according to their output semantics. Non-equality predicates still use a heuristic total-domain penalty.
template <>
double CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {
	double result;
	auto it = relation_set_2_cardinality.find(new_set);
	if (it != relation_set_2_cardinality.end()) {
		result = it->second.cardinality_before_filters;
	} else {
		// can happen if a table has cardinality 0, or a tdom is set to 0
		auto denom = GetDenominator(new_set);
		// we pass numerator relations, because for semi and anti joins, we don't want to
		// include cardinalities of relations on the RHS of a semi/anti join.
		auto numerator = GetNumerator(denom.numerator_relations);
		result = numerator / denom.denominator;
		relation_set_2_cardinality[new_set] = CardinalityHelper(result);
	}
	return ApplyOrFilterSelectivities(new_set, result);
}

double CardinalityEstimator::ApplyOrFilterSelectivities(JoinRelationSet &new_set, double cardinality) const {
	for (auto &filter : or_filters) {
		if (JoinRelationSet::IsSubset(new_set, filter->set.get())) {
			cardinality *= RelationStatisticsHelper::DEFAULT_SELECTIVITY;
		}
	}
	return cardinality;
}

template <>
idx_t CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {
	auto cardinality_as_double = EstimateCardinalityWithSet<double>(new_set);
	auto max = NumericLimits<idx_t>::Maximum();
	if (cardinality_as_double >= (double)max) {
		return max;
	}
	return (idx_t)cardinality_as_double;
}

bool SortTdoms(const RelationsSetToStats &a, const RelationsSetToStats &b) {
	if (a.has_distinct_count_hll && b.has_distinct_count_hll) {
		return a.distinct_count_hll > b.distinct_count_hll;
	}
	if (a.has_distinct_count_hll) {
		return a.distinct_count_hll > b.distinct_count_no_hll;
	}
	if (b.has_distinct_count_hll) {
		return a.distinct_count_no_hll > b.distinct_count_hll;
	}
	return a.distinct_count_no_hll > b.distinct_count_no_hll;
}

void CardinalityEstimator::InitCardinalityEstimatorProps(optional_ptr<JoinRelationSet> set, RelationStats &stats) {
	// Get the join relation set
	D_ASSERT(stats.stats_initialized);
	auto relation_cardinality = stats.cardinality;

	auto card_helper = CardinalityHelper((double)relation_cardinality);
	relation_set_2_cardinality[*set] = card_helper;

	UpdateTotalDomains(set, stats);

	// sort relations from greatest tdom to lowest tdom.
	std::sort(relation_set_stats.begin(), relation_set_stats.end(), SortTdoms);
}

void CardinalityEstimator::UpdateTotalDomains(optional_ptr<JoinRelationSet> set, RelationStats &stats) {
	D_ASSERT(set->count == 1);
	auto relation_id = set->relations[0];
	//! Initialize the distinct count for all columns used in joins with the current relation.
	//	D_ASSERT(stats.column_distinct_count.size() >= 1);

	for (idx_t i = 0; i < stats.column_distinct_count.size(); i++) {
		//! for every column used in a filter in the relation, get the distinct count via HLL, or assume it to be
		//! the cardinality
		// Update the relation_to_tdom set with the estimated distinct count (or tdom) calculated above
		auto key = ColumnBinding(TableIndex(relation_id.index), ProjectionIndex(i));
		auto distinct_count = stats.column_distinct_count.at(i);
		for (auto &relation_to_tdom : relation_set_stats) {
			const auto &i_set = relation_to_tdom.equivalent_relations;
			if (i_set.find(key) == i_set.end()) {
				continue;
			}
			if (distinct_count.from_hll && relation_to_tdom.has_distinct_count_hll) {
				relation_to_tdom.distinct_count_hll =
				    MaxValue(relation_to_tdom.distinct_count_hll, distinct_count.distinct_count);
			} else if (distinct_count.from_hll && !relation_to_tdom.has_distinct_count_hll) {
				relation_to_tdom.has_distinct_count_hll = true;
				relation_to_tdom.distinct_count_hll = distinct_count.distinct_count;
			} else {
				relation_to_tdom.distinct_count_no_hll =
				    MinValue(distinct_count.distinct_count, relation_to_tdom.distinct_count_no_hll);
			}
		}
	}
}

// LCOV_EXCL_START

void CardinalityEstimator::AddRelationNamesToRelationStats(vector<RelationStats> &stats) {
#ifdef DEBUG
	for (auto &total_domain : relation_set_stats) {
		for (auto &binding : total_domain.equivalent_relations) {
			D_ASSERT(binding.table_index.index < stats.size());
			string column_name;
			if (binding.column_index < stats[binding.table_index.index].column_names.size()) {
				column_name = stats[binding.table_index.index].column_names[binding.column_index];
			} else {
				column_name = "[unknown]";
			}
			total_domain.column_names.push_back(column_name);
		}
	}
#endif
}

void CardinalityEstimator::PrintRelationStats() {
	for (auto &total_domain : relation_set_stats) {
		string domain = "Following columns have the same distinct count: ";
		for (auto &column_name : total_domain.column_names) {
			domain += column_name + ", ";
		}
		bool have_hll = total_domain.has_distinct_count_hll;
		domain += "\n TOTAL DOMAIN = " +
		          to_string(have_hll ? total_domain.distinct_count_hll : total_domain.distinct_count_no_hll);
		Printer::Print(domain);
	}
}

// LCOV_EXCL_STOP

} // namespace duckdb
